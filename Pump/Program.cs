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
        private class WordInfo
        {
            public int mTermFreq
                = 0;
            public Dictionary<int, double> mTfIdf
                = new Dictionary<int, double>();
            public Dictionary<int, string> mMostFrequentForm
                = new Dictionary<int, string>();
        }

        private const int WINDOW_SIZE_STEP
            = 5; // in minutes
        private const int WINDOW_SIZE_DAY
            = 24 * 60;
        private const int WINDOW_SIZE_WEEK
            = 7 * 24 * 60;
        private const int WINDOW_SIZE_MONTH
            = 30 * 24 * 60;

        private static int mCommandTimeout
            = Convert.ToInt32(Latino.Utils.GetConfigValue("CommandTimeout", "0"));

        static Dictionary<int, Pair<IncrementalBowSpace, Queue<DateTime>>> mQueues
            = new Dictionary<int, Pair<IncrementalBowSpace, Queue<DateTime>>>();

        static void GetTimeSlot(DateTime time, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)WINDOW_SIZE_STEP);
            TimeSpan timeOffset = new TimeSpan(0, n * WINDOW_SIZE_STEP, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, WINDOW_SIZE_STEP, 0);
        }

        static Pair<IncrementalBowSpace, Queue<DateTime>> GetQueueInfo(int windowSize)
        {
            Pair<IncrementalBowSpace, Queue<DateTime>> queueInfo;
            if (!mQueues.TryGetValue(windowSize, out queueInfo))
            {
                queueInfo = new Pair<IncrementalBowSpace, Queue<DateTime>>();
                queueInfo.First = Utils.CreateBowSpace();
                queueInfo.Second = new Queue<DateTime>();
                mQueues.Add(windowSize, queueInfo);
            }
            return queueInfo;
        }

        static void UpdateQueue(int windowSize, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets)
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
            int num = 0;
            while (queueInfo.Second.Peek() < timeStart)
            {
                queueInfo.Second.Dequeue();
                num++;
            }
            queueInfo.First.Dequeue(num);
            //queueInfo.First.UpdateMostFrequentWordForms(); // *** too slow
        }

        static void ProcessTweets(DateTime timeStart, DateTime timeEnd, ArrayList<Pair<DateTime, string>> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0:HH:mm:ss}-{1:HH:mm:ss} ({2} tweets) ...", timeStart, timeEnd, tweets.Count);
            Dictionary<string, WordInfo> words = new Dictionary<string, WordInfo>();
            foreach (int windowSize in new int[] { WINDOW_SIZE_STEP, WINDOW_SIZE_DAY, WINDOW_SIZE_WEEK, WINDOW_SIZE_MONTH })
            {
                UpdateQueue(windowSize, timeEnd, tweets);
                IncrementalBowSpace bowSpc = GetQueueInfo(windowSize).First;
                if (windowSize == WINDOW_SIZE_STEP) // compute TF weights
                {
                    ArrayList<SparseVector<double>> bows = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cutLowWeightsPerc=*/0, /*minWordFreq=*/1);
                    SparseVector<double> bowSum = ModelUtils.ComputeCentroid(bows, CentroidType.Sum);
                    foreach (IdxDat<double> item in bowSum)
                    {
                        Word word = bowSpc.Words[item.Idx];
                        WordInfo wordInfo = new WordInfo();
                        wordInfo.mTermFreq = (int)item.Dat;
                        words.Add(word.Stem, wordInfo);
                    }
                }
                else // compute TF-IDF weights
                {
                    double wgtSum = 0;
                    int i = 0;
                    ArrayList<int> tmp = new ArrayList<int>();
                    WordInfo wordInfo;
                    foreach (Word word in bowSpc.Words)
                    {                        
                        if (word != null && words.TryGetValue(word.Stem, out wordInfo))
                        {
                            double idf = Math.Log((double)bowSpc.Count / (double)word.DocFreq);
                            double tfIdf = wordInfo.mTermFreq * idf;
                            wordInfo.mTfIdf.Add(windowSize, tfIdf);
                            wordInfo.mMostFrequentForm.Add(windowSize, word.MostFrequentForm);
                            wgtSum += tfIdf;
                            tmp.Add(i);
                        }
                        i++;
                    }
                    if (wgtSum > 0) // normalize TF-IDF weights
                    {
                        foreach (int idx in tmp)
                        {
                            Word word = bowSpc.Words[idx];
                            if (word != null && words.TryGetValue(word.Stem, out wordInfo))
                            {
                                wordInfo.mTfIdf[windowSize] /= wgtSum;
                            }
                        }
                    }
                }
            }
            // write data to database
            DataTable tbl = Utils.CreateBowTable();
            foreach (KeyValuePair<string, WordInfo> item in words)
            {
                string stem = item.Key.ToUpper();
                string word = item.Value.mMostFrequentForm[WINDOW_SIZE_MONTH].ToUpper();
                tbl.Rows.Add(
                    timeStart,
                    timeEnd,
                    Latino.Utils.Truncate(stem, 140),
                    Latino.Utils.Truncate(word, 140),
                    item.Value.mTermFreq,
                    tweets.Count,
                    item.Value.mTfIdf[WINDOW_SIZE_DAY],
                    item.Value.mTfIdf[WINDOW_SIZE_WEEK],
                    item.Value.mTfIdf[WINDOW_SIZE_MONTH],
                    word.Contains("@"),
                    word.Contains("#"),
                    word.Contains("$"),
                    word.Contains(" "),
                    null,
                    null,
                    null
                    );
            }
            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.BulkCopyTimeout = mCommandTimeout;
            bulkCopy.DestinationTableName = "BagsOfWords";
            bulkCopy.WriteToServer(tbl);
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
