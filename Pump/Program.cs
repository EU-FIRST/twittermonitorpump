using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Security.Cryptography;
using System.IO;
using System.Threading;
using System.Text;
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
            public IncrementalKMeansClustering mClustering;

            public Queue(BinarySerializer reader) : this(-1)
            {
                Load(reader);
            }

            public Queue(double clusterQualityThresh)
            {
                mClustering = Utils.CreateClustering(clusterQualityThresh);
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

        class Task
        {
            public string mScope;
            public int mStepSizeMinutes
                = 60;
            public int mWindowSizeMinutes
                = 1440;
            public Set<string> mTaggedWords
                = new Set<string>();
            public double mClusterQualityThresh
                = 0.2;
            public Queue mQueue;
            public Guid mTableId;

            public bool mRestart;

            private string mStateBinFileName;
            private string mStateBakFileName;            

            private DateTime mLastSavedStateTime
                = DateTime.MinValue;

            public Task(string scope, int stepSizeMinutes, int windowSizeMinutes, IEnumerable<string> taggedWords, double clusterQualityThresh, bool restart) 
            {
                mScope = scope;
                mStepSizeMinutes = stepSizeMinutes;
                mWindowSizeMinutes = windowSizeMinutes;
                if (taggedWords != null) { mTaggedWords.AddRange(taggedWords.Select(x => x.ToUpper())); }
                mClusterQualityThresh = clusterQualityThresh;
                mRestart = restart;                
                mQueue = new Queue(clusterQualityThresh);
                // table ID
                ArrayList<byte> buffer = new ArrayList<byte>();
                buffer.AddRange(Encoding.UTF8.GetBytes(mScope));
                buffer.AddRange(BitConverter.GetBytes(mStepSizeMinutes));
                buffer.AddRange(BitConverter.GetBytes(mWindowSizeMinutes));
                buffer.AddRange(BitConverter.GetBytes(mClusterQualityThresh));
                mTableId = new Guid(MD5.Create().ComputeHash(buffer.ToArray()));
                // state file names
                mStateBinFileName = string.Format("state_{0:N}.bin", mTableId);
                mStateBakFileName = mStateBinFileName + ".bak";
            }

            public void SaveState(long tweetId)
            {
                DateTime stateTime = mQueue.mTimeStamps.Last();
                TimeSpan diff = stateTime - mLastSavedStateTime;
                if (diff >= Utils.Config.SaveStateTimeDiff)
                {
                    Console.WriteLine("Saving state ...");
                    if (File.Exists(mStateBakFileName)) { File.Delete(mStateBakFileName); } // delete BAK file
                    if (File.Exists(mStateBinFileName)) { File.Copy(mStateBinFileName, mStateBakFileName); } // rename state file to BAK
                    BinarySerializer bs = new BinarySerializer(mStateBinFileName, FileMode.Create);
                    bs.WriteLong(tweetId); // last processed tweet ID
                    mQueue.Save(bs); // state            
                    bs.Close();
                    mLastSavedStateTime = mQueue.mTimeStamps.Last();
                }
            }

            public void InitState(out long lastId)
            {
                lastId = 0;
                if (File.Exists(mStateBakFileName))
                {
                    Console.WriteLine("Restoring state ...");
                    BinarySerializer bs = new BinarySerializer(mStateBakFileName, FileMode.Open);
                    lastId = bs.ReadLong();
                    mQueue.Load(bs);
                    bs.Close();
                    mLastSavedStateTime = mQueue.mTimeStamps.Last();
                }
            }

            public void DeleteState()
            {
                if (File.Exists(mStateBakFileName)) 
                { 
                    File.Delete(mStateBakFileName); 
                }
                if (File.Exists(mStateBinFileName)) 
                { 
                    File.Delete(mStateBinFileName); 
                }
            }
        }

        static void GetTimeSlot(DateTime time, int stepSize, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)stepSize);
            TimeSpan timeOffset = new TimeSpan(0, n * stepSize, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, stepSize, 0);
        }

        static void UpdateBowSpace(Task task, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, out int numOutdated)
        {
            IncrementalBowSpace bowSpc = task.mQueue.mBowSpace;
            Queue<DateTime> timeStamps = task.mQueue.mTimeStamps;
            DateTime timeStart = timeEnd - new TimeSpan(0, task.mWindowSizeMinutes, 0);
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

        static int SwitchRecordState(DateTime timeStart, Guid tableId, SqlConnection connection)
        {
            string cmdTxt = LUtils.GetManifestResourceString(typeof(Program), "SwitchState.sql");
            using (SqlTransaction tran = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                using (SqlCommand cmd = new SqlCommand(cmdTxt, connection, tran))
                {
                    Utils.AssignParamsToCommand(cmd, "StartTime", timeStart, "TableId", tableId);
                    cmd.CommandTimeout = Utils.Config.CommandTimeout;
                    int rowsAffected = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return rowsAffected;
                }
            }
        }

        static void ProcessTweets(Task task, DateTime timeStart, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0} {1:yyyy-MM-dd HH:mm:ss}-{2:HH:mm:ss} ({3} tweets) ...", task.mScope, timeStart, timeEnd, tweets.Count);
            DataTable clustersTable = Utils.CreateClustersTable();
            DataTable termsTable = Utils.CreateTermsTable();
            // update BOW space
            int numOutdated;
            UpdateBowSpace(task, timeEnd, tweets, out numOutdated);
            // update clusters
            IncrementalBowSpace bowSpc = task.mQueue.mBowSpace;
            IncrementalKMeansClustering clustering = task.mQueue.mClustering;
            ArrayList<SparseVector<double>> bowsTf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cut=*/0, /*minWordFreq=*/1);
            ArrayList<SparseVector<double>> bowsTfIdf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, /*cut=*/0, /*minWordFreq=*/1);
            bool useTf = bowSpc.Count < 5;
            ClusteringResult result = clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));            
            int state = 0;
            // check if time period already in DB and change state to 1
            using (SqlCommand checkExists = new SqlCommand(LUtils.GetManifestResourceString(typeof(Program), "Check.sql"), connection))
            {
                Utils.AssignParamsToCommand(checkExists, "StartTime", timeStart, "TableId", task.mTableId); 
                checkExists.CommandTimeout = Utils.Config.CommandTimeout;
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
                SparseVector<double> centroidTfIdf = useTf ? // if there is less than 5 tweets in BOW space, compute TF weights instead of TFIDF
                    ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.NrmL2) : 
                    ModelUtils.ComputeCentroid(clusterBowsTfIdf, CentroidType.NrmL2);
                Guid clusterId = ComputeClusterId(timeStart, topicId);
                clustersTable.Rows.Add(
                    task.mTableId,
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
                    bool tagged = word.Split(' ').Count(x => task.mTaggedWords.Contains(x.ToUpper())) > 0;
                    termsTable.Rows.Add(
                        task.mTableId,
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
            bulkCopy.BulkCopyTimeout = Utils.Config.CommandTimeout;
            bulkCopy.DestinationTableName = "Clusters";
            bulkCopy.WriteToServer(clustersTable);
            bulkCopy.DestinationTableName = "Terms"; 
            bulkCopy.WriteToServer(termsTable);
            bulkCopy.Close();
            if (state == 1)
            {
                Console.WriteLine("Switching record states ...");
                SwitchRecordState(timeStart, task.mTableId, connection);
            }
        }

        static void ExecSqlScript(string name, params object[] cmdParams)
        {
            string sqlTxt = LUtils.GetManifestResourceString(typeof(Program), name);
            sqlTxt = Regex.Replace(sqlTxt, "^GO", "", RegexOptions.Multiline);
            using (SqlConnection connection = new SqlConnection(Utils.Config.OutputConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(sqlTxt, connection))
                {
                    Utils.AssignParamsToCommand(cmd, cmdParams);
                    cmd.CommandTimeout = Utils.Config.CommandTimeout;
                    cmd.ExecuteNonQuery();
                }
            }        
        }

        static void ProcessTask(object objTask)
        {
            Task task = (Task)objTask;
            if (task.mRestart)
            {
                Console.WriteLine("Initializing ...");
                ExecSqlScript("Initialize.sql", "TableId", task.mTableId);
                task.DeleteState();
            }
            else
            {
                Console.WriteLine("Continuing ...");
                ExecSqlScript("Cleanup.sql", "TableId", task.mTableId);            
            }
            long lastId;
            task.InitState(out lastId);
            ArrayList<Pair<DateTime, string>> tweets = new ArrayList<Pair<DateTime, string>>();
            DateTime timeStart = DateTime.MinValue;
            DateTime timeEnd = DateTime.MinValue;
            using (SqlConnection output = new SqlConnection(Utils.Config.OutputConnectionString))
            {
                output.Open();
                using (SqlConnection input = new SqlConnection(Utils.Config.InputConnectionString))
                {
                    input.Open();
                    Console.WriteLine("Connected.");
                    using (SqlCommand cmd = new SqlCommand(LUtils.GetManifestResourceString(typeof(Program), "Read.sql"), input))
                    {
                        cmd.CommandTimeout = Utils.Config.CommandTimeout;
                        Utils.AssignParamsToCommand(cmd, "Id", lastId, "IdStr", task.mScope); 
                        SqlDataReader reader = cmd.ExecuteReader();
                        Console.WriteLine("Executed SQL reader. Reading data ...");
                        while (reader.Read())
                        {
                            long id = Utils.GetVal<long>(reader, "Id");
                            string text = Utils.GetVal<string>(reader, "Text");
                            text = HttpUtility.HtmlDecode(Utils.RemoveUrls(text)); // prepare tweet text                            
                            DateTime timeStamp = Utils.GetVal<DateTime>(reader, "CreatedAt");
                            DateTime tmpTimeStart, tmpTimeEnd;
                            GetTimeSlot(timeStamp, task.mStepSizeMinutes, out tmpTimeStart, out tmpTimeEnd);
                            if (tmpTimeStart != timeStart && timeStart != DateTime.MinValue)
                            {
                                if (tmpTimeStart < timeStart) // skip tweets with earlier time stamps
                                {
                                    Console.WriteLine("*** Tweet with earlier time stamp detected and skipped.");
                                    continue;
                                }
                                ProcessTweets(task, timeStart, timeEnd, tweets, output);
                                tweets.Clear();
                                task.SaveState(lastId);
                            }
                            timeStart = tmpTimeStart;
                            timeEnd = tmpTimeEnd;
                            tweets.Add(new Pair<DateTime, string>(timeStamp, text));
                            lastId = id;
                        }
                        if (tweets.Count > 0)
                        {
                            ProcessTweets(task, timeStart, timeEnd, tweets, output);
                            // this record is most likely incomplete; therefore don't save the state
                        }
                    }
                }
            }            
            // enqueue self
            task.mRestart = false;
            ThreadPool.QueueUserWorkItem(ProcessTask, task);
        }

        static void Main(string[] args)
        {
            ExecSqlScript("CreateTables.sql");
            Task task = new Task("GOOG", 60, 10080, "$GOOG,GOOG,GOOGLE".Split(','), 0.2, /*restart=*/true);
            Task task2 = new Task("AAPL", 60, 10080, "$AAPL,AAPL,APPLE".Split(','), 0.2, /*restart=*/true);
            ThreadPool.QueueUserWorkItem(ProcessTask, task);
            ThreadPool.QueueUserWorkItem(ProcessTask, task2);
            while (true) { Thread.Sleep(1000); }
        }
    }
}
