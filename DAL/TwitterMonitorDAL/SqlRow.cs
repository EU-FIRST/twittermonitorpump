using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace TwitterMonitorDAL
{
    public class SqlRow
    {
        public class EntityInfoDetail
        {
            public string Entity { get; set; }
            public string WindowSize { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
            public int NumOfDataPoints { get; set; }
            public int TimeSpanResolutionSec { get; set; }
        }

        public class WeightedTerm
        {
            public string Term { get; set; }
            public double Weight { get; set; }
        }

        public class TopicWeightedTerm
        {
            public long TopicId { get; set; }
            public int NumDocs { get; set; }
            public string Term { get; set; }
            public double Weight { get; set; }
        }

        public class TopicTimeSlot
        {
            public long TopicId { get; set; }
            public int TopicNumDocs { get; set; }
            public int TimeSlotNumDocs { get; set; }
            public int TimeSlotGroup { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
            public string Term { get; set; }
            public double Weight { get; set; }
        }

        public class SentimentTimelineDay
        {
            public DateTime Date { get; set; }
            public int Positive { get; set; }
            public int Negative { get; set; }
            public int NeutralPositiveBiased { get; set; }
            public int NeutralNegativeBiased { get; set; }
        }
        public class TweetSentiment
        {
            public long TweetId { get; set; }
            public DateTime? Date { get; set; }
            public string Text { get; set; }
            public string UserName { get; set; }
            public double SentimentScore { get; set; }
        }
    }
}