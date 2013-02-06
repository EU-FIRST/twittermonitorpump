﻿using System;
using System.Web;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Security.Cryptography;
using Latino;
using Latino.TextMining;
using Latino.Model;

namespace TwitterMonitorPump
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
                //mClustering.Logger.LocalLevel = Logger.Level.Trace;
                mClustering.Logger.LocalLevel = Logger.Level.Off;
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
        static string mOutputTableNameTerms
            = Latino.Utils.GetConfigValue("OutputTableNameTerms");
        static string mOutputTableNameClusters
            = Latino.Utils.GetConfigValue("OutputTableNameClusters");
        static int mWindowSize
            = Convert.ToInt32(Latino.Utils.GetConfigValue("WindowSizeMinutes", "1440"));

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
                        Latino.Utils.GetStringHashCode128(stem),
                        Latino.Utils.Truncate(stem.ToUpper(), 140),
                        Latino.Utils.Truncate(word.ToUpper(), 140),
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
