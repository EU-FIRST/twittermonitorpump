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
        TermUnigram = 1,
        TermBigram = 2,
        UserUnigram = 4,
        HashtagUnigram = 8,
        HashtagBigram = 16,
        StockUnigram = 32,
        StockBigram = 64,
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

}