using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Security.Cryptography;
using System.IO;
using System.Threading;
using System.Text.RegularExpressions;
using Latino;
using Latino.TextMining;
using Latino.Model;
using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    class Program
    {
        class Queue : ISerializable
        {
            public Queue<DateTime> mTimeStamps
                = new Queue<DateTime>();
            public IncrementalBowSpace mBowSpace
                = Utils.CreateBowSpace();
            public IncrementalKMeansClustering mClustering
                = Utils.CreateClustering(Program.mClusterQualityThresh);

            public Queue(BinarySerializer reader) : this()
            {
                Load(reader);
            }

            public Queue()
            {
                // *** for debugging only
                mClustering.BowSpace = mBowSpace;
                //mClustering.Logger.LocalLevel = Logger.Level.Trace;
                mClustering.Logger.LocalLevel = Logger.Level.Off;
            }

            public void Save(BinarySerializer writer)
            {
                new ArrayList<long>(mTimeStamps.Select(x => x.ToBinary())).Save(writer);
                mBowSpace.Save(writer);
                mClustering.Save(writer);
            }

            public void Load(BinarySerializer reader)
            {
                mTimeStamps = new Queue<DateTime>(new ArrayList<long>(reader).Select(x => DateTime.FromBinary(x)));
                mBowSpace.Load(reader);
                mClustering.Load(reader);
            }
        }

        static int mCommandTimeout
            = Convert.ToInt32(LUtils.GetConfigValue("CommandTimeout", "0"));
        static string mTopic
            = LUtils.GetConfigValue("Topic");
        static int mStepSize
            = Convert.ToInt32(LUtils.GetConfigValue("StepSizeMinutes", "60"));
        static double mBowWeightsCut
            = Convert.ToDouble(LUtils.GetConfigValue("BowWeightsCut", "0"));        
        static int mWindowSize
            = Convert.ToInt32(LUtils.GetConfigValue("WindowSizeMinutes", "1440"));
        static Set<string> mTaggedWords
            = new Set<string>(LUtils.GetConfigValue("TaggedWords", "").Split(',').Select(x => x.ToUpper()));
        static double mClusterQualityThresh
            = Convert.ToDouble(LUtils.GetConfigValue("ClusterQualityThresh", "0.1"));
        static int mSleepSeconds
            = Convert.ToInt32(LUtils.GetConfigValue("SleepSeconds", "900"));
        static string mOutputTableNameSuffix
            = LUtils.GetConfigValue("OutputTableNameSuffix", "");
        static string mTableSuffix
            = string.Format("_{0}_{1}_{2:0}", mOutputTableNameSuffix, MapWindowSize(mWindowSize), mClusterQualityThresh * 10000);
        static Guid mTableId
            = LUtils.GetStringHashCode128(string.Format("{0} {1} {2}", mOutputTableNameSuffix, mWindowSize, mClusterQualityThresh));
        static string mOutputTableNameTerms
            = "Terms" + mTableSuffix;
        static string mOutputTableNameClusters
            = "Clusters" + mTableSuffix;
        static int mSaveStateNumSteps
            = Convert.ToInt32(LUtils.GetConfigValue("SaveStateNumSteps", "10"));
        static string mOutputConnectionString
            = LUtils.GetConfigValue("OutputConnectionString");
        static string mInputConnectionString
            = LUtils.GetConfigValue("InputConnectionString");
        static string mInputSelectStatement
            = LUtils.GetConfigValue("InputSelectStatement");
        static bool mRealtime
            = true;

        static Queue mQueue
            = new Queue();

        static string MapWindowSize(int ws)
        {
            if (ws == 1440) { return "D"; }
            else if (ws == 10080) { return "W"; }
            else if (ws == 20160) { return "2W"; }
            else if (ws == 43200) { return "M"; }  
            else return ws.ToString();
        }

        static void GetTimeSlot(DateTime time, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)mStepSize);
            TimeSpan timeOffset = new TimeSpan(0, n * mStepSize, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, mStepSize, 0);
        }

        static void UpdateBowSpace(int windowSize, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, out int numOutdated)
        {
            IncrementalBowSpace bowSpc = mQueue.mBowSpace;
            Queue<DateTime> timeStamps = mQueue.mTimeStamps;
            DateTime timeStart = timeEnd - new TimeSpan(0, windowSize, 0);
            // add new tweets
            foreach (DateTime timeStamp in tweets.Select(x => x.First))
            {
                timeStamps.Enqueue(timeStamp);
            }
            bowSpc.Enqueue(tweets.Select(x => x.Second));
            // remove outdated tweets
            numOutdated = 0;
            while (timeStamps.Peek() < timeStart)
            {
                timeStamps.Dequeue();
                numOutdated++;
            }
            bowSpc.Dequeue(numOutdated);
        }

        static Guid ComputeClusterId(DateTime startTime, long topicId)
        {
            ArrayList<byte> buffer = new ArrayList<byte>();
            buffer.AddRange(BitConverter.GetBytes(startTime.ToBinary()));
            buffer.AddRange(BitConverter.GetBytes(topicId));
            return new Guid(MD5.Create().ComputeHash(buffer.ToArray()));
        }

        static int SwitchRecordState(DateTime timeStart, SqlConnection connection)
        {
            string cmdTxt = string.Format(@"
                UPDATE {0} SET RecordState = 2 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0
                UPDATE {0} SET RecordState = 0 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 1
                UPDATE {1} SET RecordState = 2 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0
                UPDATE {1} SET RecordState = 0 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 1
                ", mOutputTableNameClusters, mOutputTableNameTerms);
            using (SqlTransaction tran = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                using (SqlCommand cmd = new SqlCommand(cmdTxt, connection, tran))
                {
                    Utils.AssignParamsToCommand(cmd, "StartTime", timeStart, "TableId", mTableId);
                    cmd.CommandTimeout = mCommandTimeout;
                    int rowsAffected = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return rowsAffected;
                }
            }
        }

        static void ProcessTweets(DateTime timeStart, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0} {1:yyyy-MM-dd HH:mm:ss}-{2:HH:mm:ss} ({3} tweets) ...", mTopic, timeStart, timeEnd, tweets.Count);
            DataTable clustersTable = Utils.CreateClustersTable();
            DataTable termsTable = Utils.CreateTermsTable();
            // update BOW space
            int numOutdated;
            UpdateBowSpace(mWindowSize, timeEnd, tweets, out numOutdated);
            // update clusters
            IncrementalBowSpace bowSpc = mQueue.mBowSpace;
            IncrementalKMeansClustering clustering = mQueue.mClustering;
            ArrayList<SparseVector<double>> bowsTf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cut=*/0, /*minWordFreq=*/1);
            ArrayList<SparseVector<double>> bowsTfIdf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, /*cut=*/0, /*minWordFreq=*/1);
            bool useTf = bowSpc.Count < 5;
            ClusteringResult result = clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));            
            int state = 0;
            // check if time period already in DB and change state to 1
            using (SqlCommand checkExists = new SqlCommand(string.Format("SELECT TOP 1 * FROM {0} WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0", mOutputTableNameClusters), connection))
            {
                Utils.AssignParamsToCommand(checkExists, "StartTime", timeStart, "TableId", mTableId); 
                checkExists.CommandTimeout = mCommandTimeout;
                if (checkExists.ExecuteScalar() != null) { state = 1; Console.WriteLine("Record state set to 1."); }
            }
            // create topic-specific centroids
            int minIdx = bowSpc.Count - tweets.Count;
            foreach (Cluster cluster in result.Roots)
            {
                long topicId = (long)cluster.ClusterInfo; // topic identifier
                Set<int> items = new Set<int>(cluster.Items.Where(x => x >= minIdx).Select(x => x - minIdx));
                if (items.Count == 0) { continue; }
                ArrayList<SparseVector<double>> clusterBowsTf
                    = new ArrayList<SparseVector<double>>(bowsTf.Where((x, i) => items.Contains(i)));
                ArrayList<SparseVector<double>> clusterBowsTfIdf
                    = new ArrayList<SparseVector<double>>(bowsTfIdf.Where((x, i) => items.Contains(i)));
                SparseVector<double> sumBowsTf = ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.Sum);
                SparseVector<double> centroidTfIdf = useTf ? // if there is less than 5 tweets in the BOW space, compute TF weights instead of TF-IDF
                    ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.NrmL2) : 
                    ModelUtils.ComputeCentroid(clusterBowsTfIdf, CentroidType.NrmL2);
                Guid clusterId = ComputeClusterId(timeStart, topicId);
                clustersTable.Rows.Add(
                    mTableId,
                    clusterId,
                    timeStart,
                    timeEnd,
                    topicId, 
                    items.Count,
                    state
                    );                
                foreach (IdxDat<double> item in centroidTfIdf) 
                {
                    Word wordObj = bowSpc.Words[item.Idx];
                    string stem = wordObj.Stem;
                    string word = wordObj.MostFrequentForm;
                    int tf = (int)sumBowsTf[item.Idx];
                    int d = clusterBowsTf.Count(x => x.ContainsAt(item.Idx));
                    double tfIdf = item.Dat;
                    bool user = word.Contains("@");
                    bool hashtag = word.Contains("#");
                    bool stock = word.Contains("$");
                    bool nGram = word.Contains(" ");
                    bool tagged = word.Split(' ').Count(x => mTaggedWords.Contains(x.ToUpper())) > 0;
                    termsTable.Rows.Add(
                        mTableId,
                        clusterId,
                        LUtils.GetStringHashCode128(stem),
                        LUtils.Truncate(stem.ToUpper(), 140),
                        LUtils.Truncate(word.ToUpper(), 140),
                        tf,
                        d,
                        tfIdf,
                        user,
                        hashtag,
                        stock,
                        nGram,
                        tagged,
                        timeStart,
                        state
                        );
                }
            }
            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.BulkCopyTimeout = mCommandTimeout;
            bulkCopy.DestinationTableName = mOutputTableNameClusters;
            bulkCopy.WriteToServer(clustersTable);
            bulkCopy.DestinationTableName = mOutputTableNameTerms;
            bulkCopy.WriteToServer(termsTable);
            bulkCopy.Close();
            if (state == 1)
            {
                Console.WriteLine("Switching record states ...");
                SwitchRecordState(timeStart, connection);
            }
        }

        static void SaveState(long tweetId)
        {
            Console.WriteLine("Saving state ...");
            if (File.Exists("state.bin.bak")) { File.Delete("state.bin.bak"); } // delete BAK file
            if (File.Exists("state.bin")) { File.Copy("state.bin", "state.bin.bak"); } // rename state file to BAK
            BinarySerializer bs = new BinarySerializer("state.bin", FileMode.Create);
            bs.WriteLong(tweetId); // last processed tweet ID
            mQueue.Save(bs); // state            
            bs.Close();
        }

        static void InitState(out long lastId)
        {
            lastId = 0;
            if (File.Exists("state.bin.bak"))
            {
                Console.WriteLine("Restoring state ...");
                BinarySerializer bs = new BinarySerializer("state.bin.bak", FileMode.Open);
                lastId = bs.ReadLong();
                mQueue.Load(bs);
                bs.Close();
            }
        }

        static void DeleteState()
        {
            if (File.Exists("state.bin.bak")) { File.Delete("state.bin.bak"); }
            if (File.Exists("state.bin")) { File.Delete("state.bin"); } 
        }

        static void CreateTables()
        {
            Console.WriteLine("Creating tables ...");
            string sqlTxt = LUtils.GetManifestResourceString(typeof(Program), "CreateTables.sql");
            sqlTxt = sqlTxt.Replace("[Clusters]", "[" + mOutputTableNameClusters + "]");
            sqlTxt = sqlTxt.Replace("[Terms]", "[" + mOutputTableNameTerms + "]");
            sqlTxt = sqlTxt.Replace("UQ_Clusters", "UQ_" + mOutputTableNameClusters);
            sqlTxt = sqlTxt.Replace("UQ_Terms", "UQ_" + mOutputTableNameTerms);
            sqlTxt = Regex.Replace(sqlTxt, "^GO", "", RegexOptions.Multiline);
            using (SqlConnection connection = new SqlConnection(mOutputConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(sqlTxt, connection))
                {
                    cmd.CommandTimeout = mCommandTimeout;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static int Cleanup()
        {
            Console.WriteLine("Cleaning up ...");
            string cmdTxt = string.Format(@"
                DELETE FROM {0} WHERE TableId = @TableId AND RecordState <> 0
                DELETE FROM {1} WHERE TableId = @TableId AND RecordState <> 0
                ", mOutputTableNameClusters, mOutputTableNameTerms);
            using (SqlConnection connection = new SqlConnection(mOutputConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(cmdTxt, connection))
                {
                    Utils.AssignParamsToCommand(cmd, "TableId", mTableId);
                    cmd.CommandTimeout = mCommandTimeout;
                    return cmd.ExecuteNonQuery();
                }
            }
        }

        static void Main(string[] args)
        {
            string param = args.Count() > 0 ? args[0].ToLower() : null;
            if (param == "create")
            {                
                CreateTables();
                DeleteState();
            }
            while (true) // main loop
            {
                int N = mSaveStateNumSteps;
                int n = N;
                Cleanup();
                long lastId;
                InitState(out lastId);
                if (lastId > 0) { n *= 2; }
                ArrayList<Pair<DateTime, string>> tweets = new ArrayList<Pair<DateTime, string>>();
                DateTime timeStart = DateTime.MinValue;
                DateTime timeEnd = DateTime.MinValue;
                using (SqlConnection output = new SqlConnection(mOutputConnectionString))
                {
                    output.Open();
                    using (SqlConnection input = new SqlConnection(mInputConnectionString))
                    {
                        input.Open();
                        Console.WriteLine("Connected.");
                        using (SqlCommand cmd = new SqlCommand(mInputSelectStatement, input))
                        {
                            cmd.CommandTimeout = mCommandTimeout;
                            Utils.AssignParamsToCommand(cmd, "Id", lastId);
                            SqlDataReader reader = cmd.ExecuteReader();
                            Console.WriteLine("Executed SQL statement. Reading data ...");
                            while (reader.Read())
                            {
                                long id = Utils.GetVal<long>(reader, "Id");
                                string text = Utils.GetVal<string>(reader, "Text");
                                text = HttpUtility.HtmlDecode(Utils.RemoveUrls(text)); // prepare tweet text                            
                                DateTime timeStamp = Utils.GetVal<DateTime>(reader, "CreatedAt");
                                DateTime tmpTimeStart, tmpTimeEnd;
                                GetTimeSlot(timeStamp, out tmpTimeStart, out tmpTimeEnd);
                                if (tmpTimeStart != timeStart && timeStart != DateTime.MinValue)
                                {
                                    if (tmpTimeStart < timeStart) // skip tweets with earlier time stamps
                                    { 
                                        Console.WriteLine("*** Tweet with earlier time stamp detected and skipped.");
                                        continue; 
                                    } 
                                    ProcessTweets(timeStart, timeEnd, tweets, output);
                                    tweets.Clear();
                                    if (--n == 0) { n = N; SaveState(lastId); }
                                }
                                timeStart = tmpTimeStart;
                                timeEnd = tmpTimeEnd;
                                tweets.Add(new Pair<DateTime, string>(timeStamp, text));
                                lastId = id;
                            }
                            if (tweets.Count > 0)
                            {
                                ProcessTweets(timeStart, timeEnd, tweets, output);
                                // this record is most likely incomplete; therefore don't save the state
                            }
                        }
                    }                    
                }
                Console.WriteLine("All done.");
                if (!mRealtime) { return; }
                Console.WriteLine("Sleeping ...");
                Thread.Sleep(mSleepSeconds * 1000);
            } // while true
        }
    }
}
