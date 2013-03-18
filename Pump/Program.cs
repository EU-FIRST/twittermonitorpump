using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Security.Cryptography;
using System.IO;
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
                = Utils.CreateClustering();

            public Queue(BinarySerializer bs) : this()
            {
                Load(bs);
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
                new ArrayList<double>(mTimeStamps.Select(x => x.ToOADate())).Save(writer);
                mBowSpace.Save(writer);
                mClustering.Save(writer);
            }

            public void Load(BinarySerializer reader)
            {
                mTimeStamps = new Queue<DateTime>(new ArrayList<double>(reader).Select(x => DateTime.FromOADate(x)));
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
        static string mOutputTableNameTerms
            = LUtils.GetConfigValue("OutputTableNameTerms");
        static string mOutputTableNameClusters
            = LUtils.GetConfigValue("OutputTableNameClusters");
        static int mWindowSize
            = Convert.ToInt32(LUtils.GetConfigValue("WindowSizeMinutes", "1440"));

        static Queue mQueue
            = new Queue();

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
            //bowSpc.UpdateMostFrequentWordForms(); // *** too slow
        }

        static Guid ComputeClusterId(DateTime startTime, long topicId)
        {
            ArrayList<byte> buffer = new ArrayList<byte>();
            buffer.AddRange(BitConverter.GetBytes(startTime.ToBinary()));
            buffer.AddRange(BitConverter.GetBytes(topicId));
            return new Guid(MD5.Create().ComputeHash(buffer.ToArray()));
        }

        static void ProcessTweets(DateTime timeStart, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0:HH:mm:ss}-{1:HH:mm:ss} ({2} tweets) ...", timeStart, timeEnd, tweets.Count);
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
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, mBowWeightsCut, /*minWordFreq=*/1);
            ClusteringResult result = clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));
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
                SparseVector<double> centroidTfIdf = ModelUtils.ComputeCentroid(clusterBowsTfIdf, CentroidType.NrmL2);
                Guid clusterId = ComputeClusterId(timeStart, topicId);
                clustersTable.Rows.Add(
                    clusterId,
                    timeStart,
                    timeEnd,
                    topicId, 
                    items.Count
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
                    //Console.WriteLine("{0} {1} tf={2} d={3} tfIdf={4:0.00} @={5} #={6} $={7} term={8} window={9}", stem, word, tf, d, tfIdf, user, hashtag, stock, nGram, windowSize);
                    termsTable.Rows.Add(
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
                        nGram
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
        }

        static void SaveState(DateTime timeEnd, long tweetId)
        {
            Console.WriteLine("Saving state ...");
            if (File.Exists("state.bin.bak")) { File.Delete("state.bin.bak"); } // delete BAK file
            if (File.Exists("state.bin")) { File.Copy("state.bin", "state.bin.bak"); } // rename state file to BAK
            BinarySerializer bs = new BinarySerializer("state.bin", FileMode.Create);
            bs.WriteLong(tweetId); // last processed tweet ID
            bs.WriteDouble(timeEnd.ToOADate()); // last written record date
            mQueue.Save(bs); // state            
            bs.Close();
            // reload (testing)
            //bs = new BinarySerializer("state.bin", FileMode.Open);
            //bs.ReadLong(); // skip
            //bs.ReadDouble(); // skip
            //mQueue.Load(bs);
            //bs.Close();
        }

        static void Main(string[] args)
        {
            int N = 10;
            int n = N;
            long lastId = 0;
            ArrayList<Pair<DateTime, string>> tweets = new ArrayList<Pair<DateTime, string>>();
            DateTime timeStart = DateTime.MinValue;
            DateTime timeEnd = DateTime.MinValue;
            using (SqlConnection output = new SqlConnection(LUtils.GetConfigValue("OutputConnectionString")))
            {
                output.Open();
                using (SqlConnection input = new SqlConnection(LUtils.GetConfigValue("InputConnectionString")))
                {
                    input.Open();
                    Console.WriteLine("Connected.");
                    using (SqlCommand cmd = new SqlCommand(LUtils.GetConfigValue("InputSelectStatement"), input))
                    {
                        cmd.CommandTimeout = mCommandTimeout;
                        cmd.Parameters.Add(new SqlParameter("Id", lastId));
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
                                ProcessTweets(timeStart, timeEnd, tweets, output);                                
                                tweets.Clear();
                                if (--n == 0) { n = N; SaveState(timeEnd, id); }
                            }
                            timeStart = tmpTimeStart;
                            timeEnd = tmpTimeEnd;
                            tweets.Add(new Pair<DateTime, string>(timeStamp, text));
                        }
                        if (tweets.Count > 0)
                        {
                            ProcessTweets(timeStart, timeEnd, tweets, output);
                            // this record is most likely incomplete; therefore don't save the state
                        }
                    }
                }
                Console.WriteLine("All done.");
            }
        }
    }
}
