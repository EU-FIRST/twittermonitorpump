using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Text;

namespace TwitterMonitorDAL
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "TwitterMonitor" in code, svc and config file together.
    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    public class TwitterMonitor
    {
        [Flags]
        [DataContract]
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
        
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<WeightedTerm> TagCloud(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTerms, string windowsSize, FilterFlag filterFlag)
        {

            filterFlag = (FilterFlag)Math.Max((int)filterFlag, 1);
            FilterFlag filterFlagEnum = (FilterFlag)filterFlag;
            if (maxNumTerms == 0) maxNumTerms = 100;
            if (windowsSize == null) windowsSize = "D";

            List<Tuple<string, string>> replacements = new List<Tuple<string, string>>(new Tuple<string, string>[]
                    {
                        new Tuple<string, string>("/*REM*/", "--"), 
                        new Tuple<string, string>("--ADD", ""),
                        new Tuple<string, string>("[AAPL_D_Terms]",string.Format("[{0}_{1}_Terms]",entity, windowsSize)), 
                        new Tuple<string, string>("[AAPL_D_Clusters]",string.Format("[{0}_{1}_Clusters]",entity, windowsSize)), 
                        new Tuple<string, string>("/*#NumTerms*/", maxNumTerms.ToString()),
                    });
            foreach (FilterFlag ff in Enum.GetValues(typeof(FilterFlag)))
            {
                if (!filterFlagEnum.HasFlag(ff))
                {
                    replacements.Add(new Tuple<string, string>(string.Format("/*REM {0}*/", ff.ToString()), "--"));
                }
            }

            object[] sqlParams = new object[]
                    {
                        (int) filterFlagEnum,
                        dateTimeStart,
                        dateTimeEnd
                    };

            List<WeightedTerm> weighetdTerms = DataProvider.GetDataWithReplace<WeightedTerm>("TagCloud.sql", replacements, sqlParams);

            return weighetdTerms.ToList();
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<Topic> Topics(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTermsPerTopic,
                                  int maxNumTopics, string windowsSize, FilterFlag filterFlag) {

            filterFlag = (FilterFlag) Math.Max((int)filterFlag, 1);
            FilterFlag filterFlagEnum = (FilterFlag) filterFlag;
            if (maxNumTermsPerTopic == 0) maxNumTermsPerTopic = 100;
            if (maxNumTopics == 0) maxNumTopics = 10;
            if (windowsSize == null) windowsSize = "D";

            List<Tuple<string, string>> replacements = new List<Tuple<string, string>>(new Tuple<string, string>[]
                {
                    new Tuple<string, string>("/*REM*/", "--"),
                    new Tuple<string, string>("--ADD", ""),
                    new Tuple<string, string>("[AAPL_D_Terms]", string.Format("[{0}_{1}_Terms]", entity, windowsSize)),
                    new Tuple<string, string>("[AAPL_D_Clusters]", string.Format("[{0}_{1}_Clusters]", entity, windowsSize)),
                    new Tuple<string, string>("/*#NumTerms*/", maxNumTermsPerTopic.ToString()),
                    new Tuple<string, string>("/*#NumTopics*/", maxNumTopics.ToString()),
                });

            object[] sqlParams = new object[]
                    {
                        (int) filterFlagEnum,
                        dateTimeStart,
                        dateTimeEnd
                    };

            List<TopicWeightedTerm> topicWeightedTerms = DataProvider.GetDataWithReplace<TopicWeightedTerm>("Topics.sql", replacements, sqlParams);

            return topicWeightedTerms
                .GroupBy(topic => new {topic.TopicId, topic.NumDocs})
                .Select(topicGroup => new Topic()
                    {
                        TopicId = topicGroup.Key.TopicId,
                        NumDocs = topicGroup.Key.NumDocs,
                        Terms = topicGroup.Select(term =>
                                                  new WeightedTerm()
                                                      {
                                                          Term = term.Term,
                                                          Weight = term.Weight
                                                      }
                                          ).ToList()
                    })
                .ToList();
        }


        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public TopicsOverTime TopicsOverTime(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, TimeSpan stepTimeSpan, 
                                            /*int maxNumTermsPerTopic,*/ int maxNumTopics, string windowsSize, FilterFlag filterFlag) {

            filterFlag = (FilterFlag)Math.Max((int)filterFlag, 1);
            FilterFlag filterFlagEnum = (FilterFlag) filterFlag;
            //if (maxNumTermsPerTopic == 0) maxNumTermsPerTopic = 100;
            if (maxNumTopics == 0) maxNumTopics = 10;
            if (windowsSize == null) windowsSize = "D";

            stepTimeSpan = new TimeSpan((int) Math.Max(Math.Round(stepTimeSpan.TotalHours), 1), 0, 0);
            dateTimeStart = DateTime.MinValue + new TimeSpan((int) (Math.Round((dateTimeStart - DateTime.MinValue).TotalHours/stepTimeSpan.TotalHours)*stepTimeSpan.TotalHours), 0, 0);
            dateTimeEnd = DateTime.MinValue + new TimeSpan((int) (Math.Round((dateTimeEnd - DateTime.MinValue).TotalHours/stepTimeSpan.TotalHours)*stepTimeSpan.TotalHours), 0, 0);

            List<Tuple<string, string>> replacements = new List<Tuple<string, string>>(new Tuple<string, string>[]
                {
                    new Tuple<string, string>("/*REM*/", "--"),
                    new Tuple<string, string>("--ADD", ""),
                    new Tuple<string, string>("[AAPL_D_Terms]", string.Format("[{0}_{1}_Terms]", entity, windowsSize)),
                    new Tuple<string, string>("[AAPL_D_Clusters]", string.Format("[{0}_{1}_Clusters]", entity, windowsSize)),
                    new Tuple<string, string>("/*#NumTopics*/", maxNumTopics.ToString()),
                });

            object[] sqlParams = new object[]
                {
                    (int) filterFlagEnum,
                    dateTimeStart,
                    dateTimeEnd,
                    (int) stepTimeSpan.TotalHours,
                };

            List<TopicTimeSlots> topicTimeSlots = DataProvider.GetDataWithReplace<TopicTimeSlots>("TopicsOverTime.sql", replacements, sqlParams);

            int minTimeSlotId = topicTimeSlots.Min(tts => tts.TimeSlotGroup);
            int maxTimeSlotId = topicTimeSlots.Max(tts => tts.TimeSlotGroup);

            Dictionary<long, TimeSlotDef> timeSlotsDef =
                Enumerable
                    .Range(minTimeSlotId, maxTimeSlotId - minTimeSlotId + 1)
                    .Select(timeSlotId =>
                            new TimeSlotDef()
                                {
                                    TimeSlotId = timeSlotId,
                                    StartTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*timeSlotId),
                                    EndTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*(timeSlotId + 1))
                                })
                    .ToDictionary(tsd => tsd.TimeSlotId, tsd => tsd);

            return new TopicsOverTime()
                {
                    TimeSlotsDef = 
                        timeSlotsDef.OrderBy(kvp=>kvp.Key).Select(kvp=>kvp.Value).ToList(),
                    Topics =
                        topicTimeSlots
                            .GroupBy(topic => new {topic.TopicId, topic.TopicNumDocs})
                            .Select(topicGroup => new TopicOverTime()
                                {
                                    TopicId = topicGroup.Key.TopicId,
                                    NumDocs = topicGroup.Key.TopicNumDocs,
                                    TimeSlots = 
                                        topicGroup
                                        .Select(timeSlot =>
                                            new TimeSlot()
                                                {
                                                    TimeSlotId = timeSlot.TimeSlotGroup,
                                                    NumDocs = timeSlot.TimeSlotNumDocs,
                                                    StartTimeDate = timeSlot.StartTime,
                                                    EndTimeDate = timeSlot.EndTime
                                                })
                                        .ToList()
                                })
                            .ToList()
                };
        }
    }

    public class TopicTimeSlots
    {
        public long TopicId { get; set; }
        public int TopicNumDocs { get; set; }
        public int TimeSlotNumDocs { get; set; }
        public int TimeSlotGroup { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }

    public class TopicWeightedTerm
    {
        public long TopicId { get; set; }
        public int NumDocs { get; set; }
        public string Term { get; set; }
        public double Weight { get; set; }
    }

    [DataContract]
    public class TopicsOverTime
    {
        [DataMember]
        public List<TimeSlotDef> TimeSlotsDef { get; set; }
        [DataMember]
        public List<TopicOverTime> Topics { get; set; }
    }

    [DataContract]
    public class TopicOverTime
    {
        [DataMember]
        public long TopicId { get; set; }
        [DataMember]
        public int NumDocs { get; set; }
        [DataMember]
        public List<TimeSlot> TimeSlots { get; set; }
        [DataMember]
        public List<WeightedTerm> Terms { get; set; }
    }

    [DataContract]
    public class TimeSlot
    {
        [DataMember]
        public long TimeSlotId { get; set; }
        [DataMember]
        public int NumDocs { get; set; }

        public DateTime StartTimeDate { get; set; }
        public DateTime EndTimeDate { get; set; }
        [DataMember]
        public string DEBUG_StartTime
        {
            get { return StartTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { StartTimeDate = DateTime.Parse(value); }
        }
        [DataMember]
        public string DEBUG_EndTime
        {
            get { return EndTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { EndTimeDate = DateTime.Parse(value); }
        }
    }

    [DataContract]
    public class TimeSlotDef
    {
        [DataMember(Order = 0)]
        public long TimeSlotId { get; set; }
        
        public DateTime StartTimeDate { get; set; }
        public DateTime EndTimeDate { get; set; }
        [DataMember(Order = 1)]
        public string StartTime {
            get { return StartTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { StartTimeDate = DateTime.Parse(value); }
        }
        [DataMember(Order = 1)]
        public string EndTime {
            get { return EndTimeDate.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"); }
            set { EndTimeDate = DateTime.Parse(value); }
        }
    }

    [DataContract]
    public class Topic
    {
        [DataMember]
        public long TopicId { get; set; }
        [DataMember]
        public int NumDocs { get; set; }
        [DataMember]
        public List<WeightedTerm> Terms { get; set; }
    }
    [DataContract]
    public class WeightedTerm
    {
        [DataMember]
        public string Term { get; set; }
        [DataMember]
        public double Weight { get; set; }
    }

}
