using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Web;

namespace TwitterMonitorDAL
{
    [Flags]
    public enum FilterFlag
    {
        TermUnigram    = 1,
        TermBigram     = 2,
        UserUnigram    = 4,
        HashtagUnigram = 8,
        HashtagBigram  = 16,
        StockUnigram   = 32,
        StockBigram    = 64,
    }

    [DataContract]
    public class WeightedTerm
    {
        [DataMember(Order = 0)]
        public string Term { get; set; }
        [DataMember(Order = 1)]
        public double Weight { get; set; }
    }

    [DataContract]
    public class Topic
    {
        [DataMember(Order = 0)]
        public long TopicId { get; set; }
        [DataMember(Order = 1)]
        public int NumDocs { get; set; }
        [DataMember(Order = 2)]
        public List<WeightedTerm> Terms { get; set; }
    }

    [DataContract]
    public class TopicOverTime
    {
        [DataMember(Order = 0)]
        public long TopicId { get; set; }
        [DataMember(Order = 1)]
        public int NumDocs { get; set; }
        [DataMember(Order = 2)]
        public List<WeightedTerm> Terms { get; set; }
        [DataMember(Order = 3)]
        public List<TimeSlot> TimeSlots { get; set; }
    }

    [DataContract]
    public class TimeSlot
    {
        [DataMember(Order = 0)]
        public int NumDocs { get; set; }

        public DateTime StartTimeDate { get; set; }
        public DateTime EndTimeDate { get; set; }
        [DataMember(Order = 1)]
        public string StartTime
        {
            get { return StartTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { StartTimeDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 2)]
        public string EndTime
        {
            get { return EndTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { EndTimeDate = DateTime.Parse(value); }
        }

        [DataMember(Order = 3)]
        public List<WeightedTerm> Terms { get; set; }
    }

    [DataContract]
    public class EntityInfo
    {
        [DataMember(Order = 0)]
        public string Entity { get; set; }
        [DataMember(Order = 1)]
        public string WindowSize { get; set; }
    }

    [DataContract]
    public class EntityInfoDetail
    {
        [DataMember(Order = 0)]
        public string Entity { get; set; }
        [DataMember(Order = 1)]
        public string WindowSize { get; set; }
        public DateTime StartTimeDate { get; set; }
        public DateTime EndTimeDate { get; set; }
        [DataMember(Order = 2)]
        public string StartTime
        {
            get { return StartTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { StartTimeDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 3)]
        public string EndTime
        {
            get { return EndTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { EndTimeDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 4)]
        public int NumOfDataPoints { get; set; }
        [DataMember(Order = 5)]
        public int TimeSpanResolutionSec { get; set; }
    }

    [DataContract]
    public class ContentMapTopic
    {
        [DataMember(Order = 0)]
        public long TopicId { get; set; }
        [DataMember(Order = 1)]
        public int NumDocs { get; set; }
        [DataMember(Order = 2)]
        public List<WeightedTerm> Terms { get; set; }
        [DataMember(Order = 3)]
        public double PosX { get; set; }
        [DataMember(Order = 4)]
        public double PosY { get; set; }
    }

    [DataContract]
    public class StockInfo
    {
        [DataMember(Order = 0)]
        public string Stock { get; set; }
    }

    [DataContract]
    public class StockInfoDetail
    {
        [DataMember(Order = 0)]
        public string Stock { get; set; }
        public DateTime StartTimeDate { get; set; }
        public DateTime EndTimeDate { get; set; }
        [DataMember(Order = 1)]
        public string StartTime
        {
            get { return StartTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { StartTimeDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 2)]
        public string EndTime
        {
            get { return EndTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { EndTimeDate = DateTime.Parse(value); }
        }
    }

    [DataContract]
    public class SentimentTimelineDay
    {
        public DateTime DateDate { get; set; }
        [DataMember(Order = 0, Name = "d")]
        public string Date
        {
            get { return DateDate.ToString("yyyy-MM-dd"); }
            set { DateDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 1, Name = "v")]
        public SentimentData SentimentValue { get; set; }
    }

    [DataContract]
    public class SentimentData
    {
        [DataMember(Order = 0, Name = "p")]
        public int Positive { get; set; }
        [DataMember(Order = 1, Name = "n")]
        public int Negative { get; set; }
        [DataMember(Order = 2, Name = "pn")]
        public int NeutralPositiveBiased { get; set; }
        [DataMember(Order = 3, Name = "nn")]
        public int NeutralNegativeBiased { get; set; }
    }

    [DataContract]
    public class TweetSentiment
    {
        public DateTime DateDate { get; set; }
        [DataMember(Order = 0, Name = "d")]
        public string Date
        {
            get { return DateDate.ToString("yyyy-MM-dd HH:mm:ss"); }
            set { DateDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 1, Name = "t")]
        public string Text { get; set; }
        [DataMember(Order = 2, Name = "u")]
        public string UserName { get; set; }
        [DataMember(Order = 3, Name = "s")]
        public double SentimentScore { get; set; }
    }

    //************* Common API data types ***************************************************

    [DataContract]
    public class Entity
    {
        [DataMember(Order = 0)]
        public string Id { get; set; }
        [DataMember(Order = 1)]
        public string Description { get; set; }
        [DataMember(Order = 2)]
        public string Features { get; set; }
    }

    [DataContract]
    public class EntityDetail
    {
        [DataMember(Order = 0)]
        public string Id { get; set; }
        [DataMember(Order = 1)]
        public string Description { get; set; }
        [DataMember(Order = 2)]
        public int NumDocuments { get; set; }
        [DataMember(Order = 3)]
        public int NumOccurrences { get; set; }

        public DateTime DataStartTimeDate { get; set; }
        [DataMember(Order = 4)]
        public string DataStartTime
        {
            get { return DataStartTimeDate == DateTime.MinValue ? "" : DataStartTimeDate.ToString("yyyy-MM-dd"); }
            set { }
        }

        public DateTime DataEndTimeDate { get; set; }
        [DataMember(Order = 5)]
        public string DataEndTime
        {
            get { return DataEndTimeDate == DateTime.MinValue ? "" : DataEndTimeDate.ToString("yyyy-MM-dd"); }
            set { }
        }

        [DataMember(Order = 6)]
        public string Features { get; set; }
    }

    [DataContract]
    public class DayData
    {
        public DateTime DateDate { get; set; }
        [DataMember(Order = 0)]
        public string Date
        {
            get { return DateDate.ToString("yyyy-MM-dd"); }
            set { }
        }
    }

    [DataContract]
    public class DayVolume : DayData
    {
        [DataMember(Order = 1)]
        public double Volume { get; set; }
    }

    [DataContract]
    public class DayIndex : DayData
    {
        [DataMember(Order = 1)]
        public double Index { get; set; }
    }

    [DataContract]
    public class DayVolumeIndex : DayData
    {
        [DataMember(Order = 1)]
        public double Volume { get; set; }
        [DataMember(Order = 2)]
        public double Index { get; set; }
    }

    [DataContract]
    public class DayIndexClasses : DayData
    {
        [DataMember(Order = 1)]
        public double Positives { get; set; }
        [DataMember(Order = 2)]
        public double PosNeutrals { get; set; }
        [DataMember(Order = 3)]
        public double NegNeutrals { get; set; }
        [DataMember(Order = 4)]
        public double Negatives { get; set; }
    }

}