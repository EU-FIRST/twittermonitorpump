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
    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    public class TwitterMonitor
    {
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<WeightedTerm> TagCloud(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTerms, string windowSize, int filterFlag)
        {
            filterFlag = Math.Max((int)filterFlag, 1);
            if (maxNumTerms == 0) maxNumTerms = 100;
            if (windowSize == null) windowSize = "D";

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTerms*/", maxNumTerms.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { (int) filterFlag, dateTimeStart, dateTimeEnd };

            List<SqlRow.WeightedTerm> weighetdTerms = DataProvider.GetDataWithReplace<SqlRow.WeightedTerm>("TagCloud.sql", strRpl, sqlParams);

            return weighetdTerms.Select(term => new WeightedTerm() { Term = term.Term, Weight = term.Weight }).ToList();
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<Topic> Topics(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTermsPerTopic,
                                  int maxNumTopics, string windowSize, int filterFlag) {

            filterFlag = Math.Max((int)filterFlag, 1);
            if (maxNumTermsPerTopic == 0) maxNumTermsPerTopic = 100;
            if (maxNumTopics == 0) maxNumTopics = 10;
            if (windowSize == null) windowSize = "D";

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTerms*/", maxNumTermsPerTopic.ToString());
            strRpl.AddReplacement("/*#NumTopics*/", maxNumTopics.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { (int)filterFlag, dateTimeStart, dateTimeEnd };

            List<SqlRow.TopicWeightedTerm> topicWeightedTerms = DataProvider.GetDataWithReplace<SqlRow.TopicWeightedTerm>("Topics.sql", strRpl, sqlParams);

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
        public List<TopicOverTime> TopicsOverTime(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, TimeSpan stepTimeSpan,
                                                  int maxNumTermsPerTopic, int maxNumTopics, string windowSize, int filterFlag)
        {

            filterFlag = Math.Max((int)filterFlag, 1);
            if (maxNumTermsPerTopic == 0) maxNumTermsPerTopic = 100;
            if (maxNumTopics == 0) maxNumTopics = 10;
            if (windowSize == null) windowSize = "D";

            stepTimeSpan = new TimeSpan((int) Math.Max(Math.Round(stepTimeSpan.TotalHours), 1), 0, 0);
            dateTimeStart = DateTime.MinValue + new TimeSpan((int) (Math.Round((dateTimeStart - DateTime.MinValue).TotalHours/stepTimeSpan.TotalHours)*stepTimeSpan.TotalHours), 0, 0);
            dateTimeEnd = DateTime.MinValue + new TimeSpan((int) (Math.Round((dateTimeEnd - DateTime.MinValue).TotalHours/stepTimeSpan.TotalHours)*stepTimeSpan.TotalHours), 0, 0);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTerms*/", maxNumTermsPerTopic.ToString());
            strRpl.AddReplacement("/*#NumTopics*/", maxNumTopics.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { (int)filterFlag, dateTimeStart, dateTimeEnd, (int) stepTimeSpan.TotalHours};

            List<SqlRow.TopicTimeSlot> topicTimeSlots = DataProvider.GetDataWithReplace<SqlRow.TopicTimeSlot>("TopicsOverTime.sql", strRpl, sqlParams);
            List<SqlRow.TopicWeightedTerm> topicTerms = DataProvider.GetDataWithReplace<SqlRow.TopicWeightedTerm>("Topics.sql", strRpl, sqlParams);

            int minTimeSlotId = topicTimeSlots.Min(tts => tts.TimeSlotGroup);
            int maxTimeSlotId = topicTimeSlots.Max(tts => tts.TimeSlotGroup);
            var timeSlotsDef =
                Enumerable
                    .Range(minTimeSlotId, maxTimeSlotId - minTimeSlotId + 1)
                    .Select(timeSlotId =>
                            new
                                {
                                    TimeSlotId = timeSlotId,
                                    StartTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*timeSlotId),
                                    EndTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*(timeSlotId + 1))
                                })
                    .ToList();

            var topicTermsDict = topicTerms
                .GroupBy(tt => tt.TopicId)
                .ToDictionary(ttGroup => ttGroup.Key, ttGroup => ttGroup);

            return topicTimeSlots
                .GroupBy(topic => new {topic.TopicId, topic.TopicNumDocs})
                .Select(topicGroup =>
                    {
                        Dictionary<int, SqlRow.TopicTimeSlot> timeSlotDict = topicGroup.ToDictionary(tg => tg.TimeSlotGroup, tg => tg);

                        return new TopicOverTime()
                            {
                                TopicId = topicGroup.Key.TopicId,
                                NumDocs = topicGroup.Key.TopicNumDocs,
                                Terms = topicTermsDict[topicGroup.Key.TopicId].Select(tt=>new WeightedTerm(){Term=tt.Term,Weight= tt.Weight}).ToList(),
                                TimeSlots = timeSlotsDef
                                    .Select(timeSlotDef =>
                                        {
                                            SqlRow.TopicTimeSlot timeSlot;
                                            timeSlotDict.TryGetValue(timeSlotDef.TimeSlotId, out timeSlot);
                                            if (timeSlot == null) timeSlot = new SqlRow.TopicTimeSlot();
                                            else
                                            {
                                                if (timeSlot.StartTime < timeSlotDef.StartTimeDate || timeSlot.EndTime > timeSlotDef.EndTimeDate)
                                                    throw new Exception("Start time of a document inside a group has starting or ending time outside the group boundary!");
                                            }
                                            return new TimeSlot()
                                                {
                                                    StartTimeDate = timeSlotDef.StartTimeDate,
                                                    EndTimeDate = timeSlotDef.EndTimeDate,
                                                    NumDocs = timeSlot.TimeSlotNumDocs,
                                                };
                                        })
                                    .ToList()
                            };
                    })
                .ToList();
        }

        
        //Helper functions
        public StringReplacer StringReplacerGetDefault(string entity, string windowSize)
        {
            var strRpl = new StringReplacer();
            strRpl.AddReplacement("/*REM*/", "--");
            strRpl.AddReplacement("--ADD", "");
            strRpl.AddReplacement("[AAPL_D_Terms]", string.Format("[{0}_{1}_Terms]", entity, windowSize));
            strRpl.AddReplacement("[AAPL_D_Clusters]", string.Format("[{0}_{1}_Clusters]", entity, windowSize));

            return strRpl;
        }
        public StringReplacer StringReplacerAddFilterFlag(StringReplacer strRpl, FilterFlag filterFlag)
        {
            foreach (FilterFlag ff in Enum.GetValues(typeof(FilterFlag)))
            {
                if (!filterFlag.HasFlag(ff))
                {
                    strRpl.AddReplacement(string.Format("/*REM {0}*/", ff.ToString()), "--");
                }
            }

            return strRpl;
        }
    }

}
