using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using Latino;
using Latino.TextMining;
using Latino.Model;

namespace BagOfWordsTest
{
    class Program
    {
        class Queue
        {
            public Queue<DateTime> mTimeStamps
                = new Queue<DateTime>();
            public IncrementalBowSpace mBowSpace
                = Utils.CreateBowSpace();
            public IncrementalKMeansClustering mClustering
                = Utils.CreateClustering();

            public Queue()
            {
                // *** for debugging only
                mClustering.BowSpace = mBowSpace;
                mClustering.Logger.LocalLevel = Logger.Level.Trace;
            }
        }

        const int WINDOW_SIZE_DAY
            = 24 * 60; // in minutes
        const int WINDOW_SIZE_WEEK
            = 7 * 24 * 60;
        const int WINDOW_SIZE_MONTH
            = 30 * 24 * 60;

        static int mCommandTimeout
            = Convert.ToInt32(Latino.Utils.GetConfigValue("CommandTimeout", "0"));
        static string mTopic
            = Latino.Utils.GetConfigValue("Topic");
        static int mStepSize
            = Convert.ToInt32(Latino.Utils.GetConfigValue("StepSizeMinutes", "60"));
        static double mBowWeightsCut
            = Convert.ToDouble(Latino.Utils.GetConfigValue("BowWeightsCut", "0"));

        static Dictionary<int, Queue> mQueues
            = new Dictionary<int, Queue>();

        static void GetTimeSlot(DateTime time, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)mStepSize);
            TimeSpan timeOffset = new TimeSpan(0, n * mStepSize, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, mStepSize, 0);
        }

        static Queue GetQueue(int windowSize)
        {
            Queue queue;
            if (!mQueues.TryGetValue(windowSize, out queue))
            {
                queue = new Queue();
                mQueues.Add(windowSize, queue);
            }
            return queue;
        }

        static void UpdateBowSpace(int windowSize, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, out int numOutdated)
        {
            Queue queue = GetQueue(windowSize);
            IncrementalBowSpace bowSpc = queue.mBowSpace;
            Queue<DateTime> timeStamps = queue.mTimeStamps;
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

        static void ProcessTweets(DateTime timeStart, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0:HH:mm:ss}-{1:HH:mm:ss} ({2} tweets) ...", timeStart, timeEnd, tweets.Count);
            DataTable bowTable = Utils.CreateBowTable();
            foreach (int windowSize in new int[] { WINDOW_SIZE_DAY, WINDOW_SIZE_WEEK, WINDOW_SIZE_MONTH })
            {
                // update BOW space
                int numOutdated;
                UpdateBowSpace(windowSize, timeEnd, tweets, out numOutdated);
                // update clusters
                Queue queue = GetQueue(windowSize);
                IncrementalBowSpace bowSpc = queue.mBowSpace;
                IncrementalKMeansClustering clustering = queue.mClustering;
                ArrayList<SparseVector<double>> bowsTf
                    = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cut=*/0, /*minWordFreq=*/1);
                ArrayList<SparseVector<double>> bowsTfIdf
                    = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, mBowWeightsCut, /*minWordFreq=*/1);
                ClusteringResult result = clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));
                // create topic-specific centroids
                int minIdx = bowSpc.Count - tweets.Count;
                foreach (Cluster cluster in result.Roots)
                {
                    string topicId = string.Format("T{0}", cluster.ClusterInfo); // topic identifier
                    Set<int> items = new Set<int>(cluster.Items.Where(x => x >= minIdx).Select(x => x - minIdx));
                    if (items.Count == 0) { continue; }
                    ArrayList<SparseVector<double>> clusterBowsTf
                        = new ArrayList<SparseVector<double>>(bowsTf.Where((x, i) => items.Contains(i)));
                    ArrayList<SparseVector<double>> clusterBowsTfIdf
                        = new ArrayList<SparseVector<double>>(bowsTfIdf.Where((x, i) => items.Contains(i)));
                    SparseVector<double> sumBowsTf = ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.Sum);
                    SparseVector<double> centroidTfIdf = ModelUtils.ComputeCentroid(clusterBowsTfIdf, CentroidType.NrmL2);
                    foreach (IdxDat<double> item in centroidTfIdf) 
                    {
                        Word wordObj = bowSpc.Words[item.Idx];
                        string stem = wordObj.Stem.ToUpper();
                        string word = wordObj.MostFrequentForm.ToUpper();
                        int tf = (int)sumBowsTf[item.Idx];
                        int d = clusterBowsTf.Where(x => x.ContainsAt(item.Idx)).Count();
                        double tfIdf = item.Dat;
                        bool user = word.Contains("@");
                        bool hashtag = word.Contains("#");
                        bool stock = word.Contains("$");
                        bool nGram = word.Contains(" ");
                        //Console.WriteLine("{0} {1} tf={2} d={3} tfIdf={4:0.00} @={5} #={6} $={7} term={8} window={9}", stem, word, tf, d, tfIdf, user, hashtag, stock, nGram, windowSize);
                        bowTable.Rows.Add(
                            timeStart,
                            timeEnd,
                            mTopic,  // topic 1
                            topicId, // topic 2
                            null,    // topic 3
                            windowSize,
                            items.Count,
                            Latino.Utils.Truncate(stem, 140),
                            Latino.Utils.Truncate(word, 140),
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
            }
            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.BulkCopyTimeout = mCommandTimeout;
            bulkCopy.DestinationTableName = "BagsOfWords";
            bulkCopy.WriteToServer(bowTable);
            bulkCopy.Close();
        }

        static void Main(string[] args)
        {            
            ArrayList<Pair<DateTime, string>> tweets = new ArrayList<Pair<DateTime, string>>();
            DateTime timeStart = DateTime.MinValue;
            DateTime timeEnd = DateTime.MinValue;
            using (SqlConnection output = new SqlConnection(Latino.Utils.GetConfigValue("OutputConnectionString")))
            {
                output.Open();
                using (SqlConnection input = new SqlConnection(Latino.Utils.GetConfigValue("InputConnectionString")))
                {
                    input.Open();
                    Console.WriteLine("Connected.");
                    using (SqlCommand cmd = new SqlCommand(Latino.Utils.GetConfigValue("InputSelectStatement"), input))
                    {
                        cmd.CommandTimeout = mCommandTimeout;
                        SqlDataReader reader = cmd.ExecuteReader();
                        Console.WriteLine("Executed SQL statement. Reading data ...");
                        while (reader.Read())
                        {
                            string text = Utils.GetVal<string>(reader, "Text");
                            text = HttpUtility.HtmlDecode(Utils.RemoveUrls(text)); // prepare tweet text
                            DateTime timeStamp = Utils.GetVal<DateTime>(reader, "CreatedAt");
                            DateTime tmpTimeStart, tmpTimeEnd;
                            GetTimeSlot(timeStamp, out tmpTimeStart, out tmpTimeEnd);
                            if (tmpTimeStart != timeStart && timeStart != DateTime.MinValue)
                            {
                                ProcessTweets(timeStart, timeEnd, tweets, output);
                                tweets.Clear();
                            }
                            timeStart = tmpTimeStart;
                            timeEnd = tmpTimeEnd;
                            tweets.Add(new Pair<DateTime, string>(timeStamp, text));
                        }
                        if (tweets.Count > 0)
                        {
                            ProcessTweets(timeStart, timeEnd, tweets, output);
                        }
                    }
                }
                Console.WriteLine("All done.");
            }
        }
    }
}
