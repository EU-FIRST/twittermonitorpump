/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    Config.cs
 *  Desc:    Configuration settings
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;

using LUtils
    = Latino.Utils;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class Config
       |
       '-----------------------------------------------------------------------
    */
    public static class Config
    {
        public static readonly TimeSpan SaveStateTimeDiff
            = TimeSpan.Parse(LUtils.GetConfigValue("SaveStateTimeDiff", "10:00:00"));
        public static readonly int CommandTimeout
            = Convert.ToInt32(LUtils.GetConfigValue("CommandTimeout", "0"));
        public static readonly int BulkCopyBatchSize
            = Convert.ToInt32(LUtils.GetConfigValue("BulkCopyBatchSize", "0"));
        public static readonly string InputConnectionString
            = LUtils.GetConfigValue("InputConnectionString");
        public static readonly string OutputConnectionString
            = LUtils.GetConfigValue("OutputConnectionString");
        public static readonly int StepSizeMinutes
            = Convert.ToInt32(LUtils.GetConfigValue("StepSizeMinutes", "60"));
        public static readonly int WindowSizeDays
            = Convert.ToInt32(LUtils.GetConfigValue("WindowSizeDays", "7"));
        public static readonly ulong ProcessorAffinity
            = Convert.ToUInt64(LUtils.GetConfigValue("ProcessorAffinity", "0"));
        public static readonly int MinWorkerThreads
            = Convert.ToInt32(LUtils.GetConfigValue("MinWorkerThreads", "-1"));
        public static readonly int MaxWorkerThreads
            = Convert.ToInt32(LUtils.GetConfigValue("MaxWorkerThreads", "-1"));
        public static readonly double SentimentClassifierConfidenceThreshold
            = Convert.ToDouble(LUtils.GetConfigValue("SentimentClassifierConfidenceThreshold", "0.2"));
        public static readonly string TasksFileName
            = LUtils.GetConfigValue("TasksFileName", "Tasks.txt");
        public static readonly double DefaultClusterQualityThreshold
            = Convert.ToDouble(LUtils.GetConfigValue("DefaultClusterQualityThreshold", "0.2"));
        public static readonly int DefaultMinTaskTimeMinutes
            = Convert.ToInt32(LUtils.GetConfigValue("DefaultMinTaskTimeMinutes", "5"));
        public static readonly DateTime MinTweetTimestamp
            = DateTime.Parse(LUtils.GetConfigValue("MinTweetTimestamp", "2000-01-01"));
        public static readonly string TableId
            = LUtils.GetConfigValue("TableId", "");
        public static readonly string AdditionalStopWords
            = LUtils.GetConfigValue("AdditionalStopWords", "");
    }
}