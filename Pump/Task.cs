/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    Task.cs
 *  Desc:    Topic tracking and sentiment analysis task
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using Latino;
using Latino.TextMining;
using Latino.Model;
using SentimentClassifier;

using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class Task
       |
       '-----------------------------------------------------------------------
    */
    class Task
    {
        public readonly string Scope;
        public readonly int StepSizeMinutes;
        public readonly Guid TableId;
        public bool Restart;

        private int mWindowSizeMinutes;
        private Set<string> mTaggedWords
            = new Set<string>();
        private double mClusterQualityThresh;
        private State mState;        

        private string mStateBinFileName;
        private string mStateBakFileName;

        private DateTime mLastSavedStateTime
            = DateTime.MinValue;

        private static ISentimentClassifier mBasicModel
            = null;

        public static void Initialize()
        {
            Console.WriteLine("Loading basic sentiment model ...");
            using (BinarySerializer reader = new BinarySerializer(LUtils.GetManifestResourceStream(typeof(ISentimentClassifier), "BasicSentimentModel.bin")))
            {
                mBasicModel = new BasicSentimentClassifier(reader);
            }
        }

        public Task(string scope, int stepSizeMinutes, int windowSizeMinutes, IEnumerable<string> taggedWords, double clusterQualityThresh, bool restart)
        {
            Scope = scope;
            StepSizeMinutes = stepSizeMinutes;
            mWindowSizeMinutes = windowSizeMinutes;
            if (taggedWords != null) { mTaggedWords.AddRange(taggedWords.Select(x => x.ToUpper())); }
            mClusterQualityThresh = clusterQualityThresh;
            Restart = restart;
            mState = new State(clusterQualityThresh);
            // table ID
            ArrayList<byte> buffer = new ArrayList<byte>();
            buffer.AddRange(Encoding.UTF8.GetBytes(Scope));
            buffer.AddRange(BitConverter.GetBytes(StepSizeMinutes));
            buffer.AddRange(BitConverter.GetBytes(mWindowSizeMinutes));
            buffer.AddRange(BitConverter.GetBytes(mClusterQualityThresh));
            TableId = new Guid(MD5.Create().ComputeHash(buffer.ToArray()));
            // state file names
            mStateBinFileName = string.Format("state_{0:N}.bin", TableId);
            mStateBakFileName = mStateBinFileName + ".bak";
        }

        public void WriteLine(string msg, params object[] args)
        {
            Console.WriteLine("[" + Scope + "] " + msg, args);
        }

        public void SaveState(long tweetId)
        {
            DateTime stateTime = mState.mTimeStamps.Last();
            TimeSpan diff = stateTime - mLastSavedStateTime;
            if (diff >= Config.SaveStateTimeDiff)
            {
                WriteLine("Saving state ...");
                if (File.Exists(mStateBakFileName)) { File.Delete(mStateBakFileName); } // delete BAK file
                if (File.Exists(mStateBinFileName)) { File.Copy(mStateBinFileName, mStateBakFileName); } // rename state file to BAK
                using (BinarySerializer bs = new BinarySerializer(mStateBinFileName, FileMode.Create))
                {
                    bs.WriteLong(tweetId); // last processed tweet ID
                    mState.Save(bs); // state            
                }
                mLastSavedStateTime = mState.mTimeStamps.Last();
            }
        }

        public void InitState(out long lastId)
        {
            lastId = 0;
            if (File.Exists(mStateBakFileName))
            {
                WriteLine("Restoring state ...");
                using (BinarySerializer bs = new BinarySerializer(mStateBakFileName, FileMode.Open))
                {
                    lastId = bs.ReadLong();
                    mState.Load(bs);
                }
                mLastSavedStateTime = mState.mTimeStamps.Last();
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

        private int SwitchRecordState(DateTime timeStart, SqlConnection connection)
        {
            string cmdTxt = LUtils.GetManifestResourceString(typeof(Program), "SwitchState.sql");
            using (SqlTransaction tran = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                using (SqlCommand cmd = new SqlCommand(cmdTxt, connection, tran))
                {
                    Utils.AssignParamsToCommand(cmd, "StartTime", timeStart, "TableId", TableId);
                    cmd.CommandTimeout = Config.CommandTimeout;
                    int rowsAffected = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return rowsAffected;
                }
            }
        }

        private void UpdateBowSpace(DateTime timeEnd, ArrayList<Tweet> tweets, out int numOutdated)
        {
            IncrementalBowSpace bowSpc = mState.mBowSpace;
            Queue<DateTime> timeStamps = mState.mTimeStamps;
            DateTime timeStart = timeEnd - new TimeSpan(0, mWindowSizeMinutes, 0);
            // add new tweets
            foreach (DateTime timeStamp in tweets.Select(x => x.CreatedAt))
            {
                timeStamps.Enqueue(timeStamp);
            }
            bowSpc.Enqueue(tweets.Select(x => x.Text));
            // remove outdated tweets
            numOutdated = 0;
            while (timeStamps.Peek() < timeStart)
            {
                timeStamps.Dequeue();
                numOutdated++;
            }
            bowSpc.Dequeue(numOutdated);
        }

        private static Guid ComputeClusterId(DateTime startTime, long topicId)
        {
            ArrayList<byte> buffer = new ArrayList<byte>();
            buffer.AddRange(BitConverter.GetBytes(startTime.ToBinary()));
            buffer.AddRange(BitConverter.GetBytes(topicId));
            return new Guid(MD5.Create().ComputeHash(buffer.ToArray()));
        }

        public void ProcessTweets(DateTime timeStart, DateTime timeEnd, ArrayList<Tweet> tweets, SqlConnection connection)
        {
            WriteLine("Processing tweets {0:yyyy-MM-dd HH:mm:ss}-{1:HH:mm:ss} ({2} tweets) ...", timeStart, timeEnd, tweets.Count);
            DataTable clustersTable = Utils.CreateClustersTable();
            DataTable termsTable = Utils.CreateTermsTable();
            DataTable tweetsTable = Utils.CreateTweetsTable();
            // update BOW space
            int numOutdated;
            UpdateBowSpace(timeEnd, tweets, out numOutdated);
            // update clusters
            IncrementalBowSpace bowSpc = mState.mBowSpace;
            IncrementalKMeansClustering clustering = mState.mClustering;
            ArrayList<SparseVector<double>> bowsTf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TermFreq, /*normalizeVectors=*/false, /*cut=*/0, /*minWordFreq=*/1);
            ArrayList<SparseVector<double>> bowsTfIdf
                = bowSpc.GetMostRecentBows(tweets.Count, WordWeightType.TfIdf, /*normalizeVectors=*/true, /*cut=*/0, /*minWordFreq=*/1);
            bool useTf = bowSpc.Count < 5;
            //bool useTf = false; 
            ClusteringResult result = clustering.Cluster(numOutdated, new UnlabeledDataset<SparseVector<double>>(bowsTfIdf));
            int state = 0;
            // check if time period already in DB and change state to 1
            using (SqlCommand checkExists = new SqlCommand(LUtils.GetManifestResourceString(typeof(Program), "Check.sql"), connection))
            {
                Utils.AssignParamsToCommand(checkExists, "StartTime", timeStart, "TableId", TableId);
                checkExists.CommandTimeout = Config.CommandTimeout;
                if (checkExists.ExecuteScalar() != null) { state = 1; WriteLine("Record state set to 1."); }
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
                ArrayList<Tweet> clusterTweets
                    = new ArrayList<Tweet>(tweets.Where((x, i) => items.Contains(i)));
                SparseVector<double> sumBowsTf = ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.Sum);
                SparseVector<double> centroidTfIdf = useTf ? // if less than 5 tweets in BOW space, compute TF weights instead of TFIDF
                    ModelUtils.ComputeCentroid(clusterBowsTf, CentroidType.NrmL2) :
                    ModelUtils.ComputeCentroid(clusterBowsTfIdf, CentroidType.NrmL2);
                Guid clusterId = ComputeClusterId(timeStart, topicId);
                int numPos = 0;
                int numNeg = 0;
                int numPosLowCfd = 0;
                int numNegLowCfd = 0;
                int numNeutral = 0;
                foreach (Tweet tweet in clusterTweets)
                {
                    double confThr = Config.SentimentClassifierConfidenceThreshold;
                    double sentiment = mBasicModel.Classify(tweet.Text);
                    bool isPos = sentiment > confThr;
                    bool isNeg = sentiment < -confThr;
                    bool isPosLowCfd = sentiment <= confThr && sentiment > 0;
                    bool isNegLowCfd = sentiment >= -confThr && sentiment < 0;
                    bool isNeutral = sentiment >= -confThr && sentiment <= confThr;
                    if (isPos) { numPos++; }
                    if (isNeg) { numNeg++; }
                    if (isPosLowCfd) { numPosLowCfd++; }
                    if (isNegLowCfd) { numNegLowCfd++; }
                    if (isNeutral) { numNeutral++; }
                    tweetsTable.Rows.Add(
                        TableId,
                        clusterId,
                        timeStart,
                        tweet.Id,
                        // basic sentiment
                        sentiment,
                        isPos,
                        isNeg,
                        isNeutral,
                        isPosLowCfd,
                        isNegLowCfd,
                        // sentiment from Sentiment SVC (TODO)
                        sentiment,
                        isPos,
                        isNeg,
                        isNeutral,
                        isPosLowCfd,
                        isNegLowCfd,
                        true,  // is basic? // TODO: false if Sentiment SVC available
                        false, // is hand-labeled? // TODO: true if Sentiment SVC reports as hand-labeled
                        state
                        );
                }
                clustersTable.Rows.Add(
                    TableId,
                    clusterId,
                    timeStart,
                    timeEnd,
                    topicId,
                    items.Count,
                    // basic sentiment
                    numPos,
                    numNeg,
                    numNeutral,
                    numPosLowCfd,
                    numNegLowCfd,
                    // sentiment from Sentiment SVC (TODO)
                    numPos,
                    numNeg,
                    numNeutral,
                    numPosLowCfd,
                    numNegLowCfd,
                    // hand-labeled sentiment (TODO)
                    0, // pos
                    0, // neg 
                    0, // neutral
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
                        TableId,
                        clusterId,
                        timeStart,
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
                        state
                        );
                }
            }
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.BulkCopyTimeout = Config.CommandTimeout;
                bulkCopy.DestinationTableName = "Clusters";
                bulkCopy.WriteToServer(clustersTable);
                bulkCopy.DestinationTableName = "Terms";
                bulkCopy.WriteToServer(termsTable);
                bulkCopy.DestinationTableName = "Tweets";
                bulkCopy.WriteToServer(tweetsTable);
            }
            if (state == 1)
            {
                WriteLine("Switching record states ...");
                SwitchRecordState(timeStart, connection);
            }
        }
    }
}
