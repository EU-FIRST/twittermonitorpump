/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    Program.cs
 *  Desc:    Real-time topic tracking and sentiment analysis
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;
using System.Web;
using System.Data.SqlClient;
using System.Threading;
using System.Diagnostics;
using Latino;

using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class Program
       |
       '-----------------------------------------------------------------------
    */
    class Program
    {
        static void GetTimeSlot(DateTime time, int stepSize, out DateTime timeStart, out DateTime timeEnd)
        {
            double min = (time - DateTime.MinValue).TotalMinutes;
            int n = (int)Math.Floor(min / (double)stepSize);
            TimeSpan timeOffset = new TimeSpan(0, n * stepSize, 0);
            timeStart = DateTime.MinValue + timeOffset;
            timeEnd = timeStart + new TimeSpan(0, stepSize, 0);
        }

        static void ProcessTask(object objTask)
        {
            Task task = (Task)objTask;
            if (task.Restart)
            {
                task.WriteLine("Initializing ...");
                Utils.ExecSqlScript("Initialize.sql", "TableId", task.TableId);
                task.DeleteState();
            }
            else
            {
                task.WriteLine("Continuing ...");
                Utils.ExecSqlScript("Cleanup.sql", "TableId", task.TableId);            
            }
            long lastId;
            task.InitState(out lastId);
            ArrayList<Tweet> tweets = new ArrayList<Tweet>();
            DateTime timeStart = DateTime.MinValue;
            DateTime timeEnd = DateTime.MinValue;
            using (SqlConnection output = new SqlConnection(Config.OutputConnectionString))
            {
                output.Open();
                using (SqlConnection input = new SqlConnection(Config.InputConnectionString))
                {
                    input.Open();
                    task.WriteLine("Connected.");
                    using (SqlCommand cmd = new SqlCommand(LUtils.GetManifestResourceString(typeof(Program), "Read.sql"), input))
                    {
                        cmd.CommandTimeout = Config.CommandTimeout;
                        Utils.AssignParamsToCommand(cmd, "Id", lastId, "IdStr", task.Scope); 
                        SqlDataReader reader = cmd.ExecuteReader();
                        task.WriteLine("Executed SQL reader. Reading data ...");
                        while (reader.Read())
                        {
                            long id = Utils.GetVal<long>(reader, "Id");
                            string text = Utils.GetVal<string>(reader, "Text");
                            text = HttpUtility.HtmlDecode(Utils.RemoveUrls(text)); // prepare tweet text                            
                            DateTime timeStamp = Utils.GetVal<DateTime>(reader, "CreatedAt");
                            DateTime tmpTimeStart, tmpTimeEnd;
                            GetTimeSlot(timeStamp, task.StepSizeMinutes, out tmpTimeStart, out tmpTimeEnd);
                            if (tmpTimeStart != timeStart && timeStart != DateTime.MinValue)
                            {
                                if (tmpTimeStart < timeStart) // skip tweets with earlier time stamps
                                {
                                    task.WriteLine("*** Tweet with earlier time stamp detected and skipped.");
                                    continue;
                                }
                                task.ProcessTweets(timeStart, timeEnd, tweets, output);
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
                            task.ProcessTweets(timeStart, timeEnd, tweets, output);
                            // this record is most likely incomplete; therefore don't save the state
                        }
                    }
                }
            }            
            // enqueue self
            task.Restart = false;
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
            if (Config.MinWorkerThreads > -1)
            {
                ThreadPool.SetMinThreads(minWt = Config.MinWorkerThreads, minCpt);
            }
            if (Config.MaxWorkerThreads > -1)
            {
                ThreadPool.SetMaxThreads(maxWt = Config.MaxWorkerThreads, maxCpt);
            }
            Console.WriteLine("Min worker threads: {0}", minWt);
            Console.WriteLine("Max worker threads: {0}", maxWt);
            Console.WriteLine();
            Console.WriteLine("Processor affinity:");
            Console.WriteLine();
            Process p = Process.GetCurrentProcess();
            if (Config.ProcessorAffinity > 0) 
            { 
                p.ProcessorAffinity = (IntPtr)Config.ProcessorAffinity; 
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
            Task.Initialize(); // loads sentiment model
            Utils.ExecSqlScript("CreateTables.sql");
            int windowSizeMinutes = Config.WindowSizeDays * 1440; 
            Task task1 = new Task("GOOG", Config.StepSizeMinutes, windowSizeMinutes, "$GOOG,GOOGLE".Split(','), 0.15, /*restart=*/true);
            Task task2 = new Task("AAPL", Config.StepSizeMinutes, windowSizeMinutes, "$AAPL,APPLE".Split(','), 0.15, /*restart=*/true);
            ThreadPool.QueueUserWorkItem(ProcessTask, task1);
            ThreadPool.QueueUserWorkItem(ProcessTask, task2);
            while (true) { Thread.Sleep(1000); }
        }
    }
}
