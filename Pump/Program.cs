using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using Latino;
using Latino.TextMining;
using Latino.Model;
using System.Data;

namespace BagOfWordsTest
{
    class Program
    {
        private const int STEP_SIZE
            = 60; // in minutes
        private const int WINDOW_SIZE_DAY
            = 24 * 60;
        private const int WINDOW_SIZE_WEEK
            = 7 * 24 * 60;
        private const int WINDOW_SIZE_MONTH
            = 30 * 24 * 60;

        private static int mCommandTimeout
            = Convert.ToInt32(Latino.Utils.GetConfigValue("CommandTimeout", "0"));

        static Dictionary<int, Pair<IncrementalBowSpace, Queue<DateTime>>> mBowQueues
            = new Dictionary<int, Pair<IncrementalBowSpace, Queue<DateTime>>>();
        static Dictionary<int, IncrementalKMeansClustering> mClusteringQueues
            = new Dictionary<int, IncrementalKMeansClustering>();

        static void GetTimeSlot(DateTime time, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)STEP_SIZE);
            TimeSpan timeOffset = new TimeSpan(0, n * STEP_SIZE, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, STEP_SIZE, 0);
        }

        static Pair<IncrementalBowSpace, Queue<DateTime>> GetQueueInfo(int windowSize)
        {
            Pair<IncrementalBowSpace, Queue<DateTime>> queueInfo;
            if (!mBowQueues.TryGetValue(windowSize, out queueInfo))
            {
                queueInfo = new Pair<IncrementalBowSpace, Queue<DateTime>>();
                queueInfo.First = Utils.CreateBowSpace();
                queueInfo.Second = new Queue<DateTime>();
                mBowQueues.Add(windowSize, queueInfo);
            }
            return queueInfo;
        }

        static IncrementalKMeansClustering GetClustering(int windowSize)
        {
            IncrementalKMeansClustering clustering;
            if (!mClusteringQueues.TryGetValue(windowSize, out clustering))
            {
                clustering = new IncrementalKMeansClustering();
                clustering.QualThresh = 0.2;
                clustering.Logger.LocalLevel = Logger.Level.Trace;
                clustering.BowSpace = GetQueueInfo(windowSize).First;
                mClusteringQueues.Add(windowSize, clustering);                
            }
            return clustering;
        }

        static void UpdateBowSpace(int windowSize, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, out int numOutdated)
        {
            Pair<IncrementalBowSpace, Queue<DateTime>> queueInfo = GetQueueInfo(windowSize);            
            DateTime timeStart = timeEnd - new TimeSpan(0, windowSize, 0);
            // add new tweets
            foreach (DateTime timeStamp in tweets.Select(x => x.First))
            {
                queueInfo.Second.Enqueue(timeStamp);
            }
            queueInfo.First.Enqueue(tweets.Select(x => x.Second));
            // remove outdated tweets
            numOutdated = 0;
            while (queueInfo.Second.Peek() < timeStart)
            {
                queueInfo.Second.Dequeue();
                numOutdated++;
            }
            queueInfo.First.Dequeue(numOutdated);
            //queueInfo.First.UpdateMostFrequentWordForms(); // *** too slow
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
                IncrementalBowSpace bowSpc = GetQueueInfo(windowSize).First;                
                IncrementalKMeansClustering clustering = GetClustering(windowSize);
                ArrayList<SparseVector<double>> bowsTfIdf 
                    = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, /*cut=*/0, /*minWordFreq=*/1); // *** cut here 
                clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));                
                // write BOWs to DB
                ArrayList<SparseVector<double>> bowsTf
                    = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cut=*/0, /*minWordFreq=*/1);
                SparseVector<double> sumBowsTf = ModelUtils.ComputeCentroid(bowsTf, CentroidType.Sum);
                SparseVector<double> centroidTfIdf = ModelUtils.ComputeCentroid(bowsTfIdf, CentroidType.NrmL2);
                foreach (IdxDat<double> item in centroidTfIdf) // *** do this for each topic-specific centroid
                {
                    Word wordObj = bowSpc.Words[item.Idx];
                    string stem = wordObj.Stem.ToUpper();
                    string word = wordObj.MostFrequentForm.ToUpper();
                    int tf = (int)sumBowsTf[item.Idx];
                    int d = 0;
                    foreach (SparseVector<double> bow in bowsTf) 
                    { 
                        if (bow.ContainsAt(item.Idx)) { d++; } 
                    }
                    double tfIdf = item.Dat;
                    bool user = word.Contains("@");
                    bool hashtag = word.Contains("#");
                    bool stock = word.Contains("$");
                    bool nGram = word.Contains(" ");
                    //Console.WriteLine("{0} {1} tf={2} d={3} tfIdf={4:0.00} @={5} #={6} $={7} term={8} window={9}", 
                    //    stem, word, tf, d, tfIdf, user, hashtag, stock, nGram, windowSize);
                    bowTable.Rows.Add(
                        timeStart,
                        timeEnd,
                        null, // topic 1
                        null, // topic 2
                        null, // topic 3
                        windowSize,
                        tweets.Count, // *** cluster size
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
                            DateTime _timeStart, _timeEnd;
                            GetTimeSlot(timeStamp, out _timeStart, out _timeEnd);
                            if (_timeStart != timeStart && timeStart != DateTime.MinValue)
                            {
                                ProcessTweets(timeStart, timeEnd, tweets, output);
                                tweets.Clear();
                            }
                            timeStart = _timeStart;
                            timeEnd = _timeEnd;
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
