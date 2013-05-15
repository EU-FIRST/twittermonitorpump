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
using System.Diagnostics;
using Latino;
using Latino.TextMining;
using Latino.Model;
using SentimentClassifier;

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
            public int mStepSizeMinutes;
            public int mWindowSizeMinutes;
            public Set<string> mTaggedWords
                = new Set<string>();
            public double mClusterQualityThresh;
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
                    using (BinarySerializer bs = new BinarySerializer(mStateBinFileName, FileMode.Create))
                    {
                        bs.WriteLong(tweetId); // last processed tweet ID
                        mQueue.Save(bs); // state            
                    }
                    mLastSavedStateTime = mQueue.mTimeStamps.Last();
                }
            }

            public void InitState(out long lastId)
            {
                lastId = 0;
                if (File.Exists(mStateBakFileName))
                {
                    Console.WriteLine("Restoring state ...");
                    using (BinarySerializer bs = new BinarySerializer(mStateBakFileName, FileMode.Open))
                    {
                        lastId = bs.ReadLong();
                        mQueue.Load(bs);
                    }
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

        static ISentimentClassifier mBasicModel
            = null;

        static void GetTimeSlot(DateTime time, int stepSize, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)stepSize);
            TimeSpan timeOffset = new TimeSpan(0, n * stepSize, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, stepSize, 0);
        }

        static void UpdateBowSpace(Task task, DateTime timeEnd, ArrayList<Tweet> tweets, out int numOutdated)
        {
            IncrementalBowSpace bowSpc = task.mQueue.mBowSpace;
            Queue<DateTime> timeStamps = task.mQueue.mTimeStamps;
            DateTime timeStart = timeEnd - new TimeSpan(0, task.mWindowSizeMinutes, 0);
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

        static void ProcessTweets(Task task, DateTime timeStart, DateTime timeEnd, ArrayList<Tweet> tweets, SqlConnection connection)
        {
            Console.WriteLine("Processing tweets {0} {1:yyyy-MM-dd HH:mm:ss}-{2:HH:mm:ss} ({3} tweets) ...", task.mScope, timeStart, timeEnd, tweets.Count);
            DataTable clustersTable = Utils.CreateClustersTable();
            DataTable termsTable = Utils.CreateTermsTable();
            DataTable tweetsTable = Utils.CreateTweetsTable();
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
            //bool useTf = false; 
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
                    double confThr = Utils.Config.SentimentClassifierConfidenceThreshold;
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
                        task.mTableId,
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
                    task.mTableId,
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
                    bool tagged = word.Split(' ').Count(x => task.mTaggedWords.Contains(x.ToUpper())) > 0;
                    termsTable.Rows.Add(
                        task.mTableId,
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
                bulkCopy.BulkCopyTimeout = Utils.Config.CommandTimeout;
                bulkCopy.DestinationTableName = "Clusters";
                bulkCopy.WriteToServer(clustersTable);
                bulkCopy.DestinationTableName = "Terms";
                bulkCopy.WriteToServer(termsTable);
                bulkCopy.DestinationTableName = "Tweets";
                bulkCopy.WriteToServer(tweetsTable);
            }
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

        /* .-----------------------------------------------------------------------
           |
           |  Class Tweet
           |
           '-----------------------------------------------------------------------
        */
        class Tweet
        {
            public readonly long Id;
            public readonly string Text;
            public readonly DateTime CreatedAt;

            public Tweet(long id, string text, DateTime createdAt)
            {
                Id = id;
                Text = text;
                CreatedAt = createdAt;
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
            ArrayList<Tweet> tweets = new ArrayList<Tweet>();
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
                            tweets.Add(new Tweet(id, text, timeStamp));
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
            Console.WriteLine("Concurrency config:");
            Console.WriteLine();
            int minWt, minCpt;
            ThreadPool.GetMinThreads(out minWt, out minCpt);
            int maxWt, maxCpt;
            ThreadPool.GetMaxThreads(out maxWt, out maxCpt);
            if (Utils.Config.MinWorkerThreads > -1)
            {
                ThreadPool.SetMinThreads(minWt = Utils.Config.MinWorkerThreads, minCpt);
            }
            if (Utils.Config.MaxWorkerThreads > -1)
            {
                ThreadPool.SetMaxThreads(maxWt = Utils.Config.MaxWorkerThreads, maxCpt);
            }
            Console.WriteLine("Min worker threads: {0}", minWt);
            Console.WriteLine("Max worker threads: {0}", maxWt);
            Console.WriteLine();
            Console.WriteLine("Processor affinity:");
            Console.WriteLine();
            Process p = Process.GetCurrentProcess();
            if (Utils.Config.ProcessorAffinity > 0) 
            { 
                p.ProcessorAffinity = (IntPtr)Utils.Config.ProcessorAffinity; 
            }
            ulong aff = (ulong)p.ProcessorAffinity;
            for (int i = 1; i <= Environment.ProcessorCount; i++) 
            {
                if ((aff & 1) == 1) { Console.WriteLine("Core {0}: yes", i); }
                else { Console.WriteLine("Core {0}: no", i); }
                aff >>= 1;
            }
            Console.WriteLine();
            Console.WriteLine("Press any key to start processing ...");
            Console.ReadKey(true);
            Console.WriteLine();
            Console.WriteLine("Loading basic sentiment model ...");
            using (BinarySerializer reader = new BinarySerializer(LUtils.GetManifestResourceStream(typeof(ISentimentClassifier), "BasicSentimentModel.bin")))
            {
                mBasicModel = new BasicSentimentClassifier(reader);
            }
            ExecSqlScript("CreateTables.sql");
            int windowSizeMinutes = Utils.Config.WindowSizeDays * 1440; 
            Task task1 = new Task("GOOG", Utils.Config.StepSizeMinutes, windowSizeMinutes, "$GOOG,GOOGLE".Split(','), 0.15, /*restart=*/true);
            Task task2 = new Task("AAPL", Utils.Config.StepSizeMinutes, windowSizeMinutes, "$AAPL,APPLE".Split(','), 0.15, /*restart=*/true);
            ThreadPool.QueueUserWorkItem(ProcessTask, task1);
            ThreadPool.QueueUserWorkItem(ProcessTask, task2);
            while (true) { Thread.Sleep(1000); }
        }
    }
}
