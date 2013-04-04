using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
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
        public List<EntityInfo> AllEntities()
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            var sqlParams = new object[] { };

            List<SqlRow.EntityInfoDetail> dataDescription = DataProvider.GetDataWithReplace<SqlRow.EntityInfoDetail>("AllEntities.sql", strRpl, sqlParams);

            return dataDescription.Select(dd => new EntityInfo()
            {
                Entity = dd.Entity,
                WindowSize = dd.WindowSize
            }).ToList();
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public EntityInfoDetail EntityDetail(string entity, string windowSize)
        {
            windowSize = ParameterChecker.WindowSize(windowSize);
            entity = ParameterChecker.Entity(entity, windowSize);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            var sqlParams = new object[] { entity, windowSize };

            List<SqlRow.EntityInfoDetail> dataDescription = DataProvider.GetDataWithReplace<SqlRow.EntityInfoDetail>("EntityDetail.sql", strRpl, sqlParams);

            return dataDescription.Select(dd => new EntityInfoDetail()
                {
                    Entity = dd.Entity,
                    WindowSize = dd.WindowSize,
                    StartTimeDate = dd.StartTime,
                    EndTimeDate = dd.EndTime,
                    NumOfDataPoints = dd.NumOfDataPoints,
                    TimeSpanResolutionSec = dd.TimeSpanResolutionSec
                }).FirstOrDefault();
        }
        
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<WeightedTerm> TagCloud(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTerms, string windowSize, int filterFlag)
        {
            windowSize = ParameterChecker.WindowSize(windowSize);
            entity = ParameterChecker.Entity(entity, windowSize);
            filterFlag = ParameterChecker.FilterFlagCheck(filterFlag);
            maxNumTerms = ParameterChecker.StrictlyPositiveNumber(maxNumTerms, 100);
            dateTimeStart = ParameterChecker.DateRound(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRound(dateTimeEnd);

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
                                  int maxNumTopics, string windowSize, int filterFlag) 
        {
            windowSize = ParameterChecker.WindowSize(windowSize);
            entity = ParameterChecker.Entity(entity, windowSize);
            filterFlag = ParameterChecker.FilterFlagCheck(filterFlag);
            maxNumTopics = ParameterChecker.StrictlyPositiveNumber(maxNumTopics, 10);
            maxNumTermsPerTopic = ParameterChecker.StrictlyPositiveNumber(maxNumTermsPerTopic, 50);
            dateTimeStart = ParameterChecker.DateRound(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRound(dateTimeEnd);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTermsPerTopic*/", maxNumTermsPerTopic.ToString());
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
                                                  int maxNumTopics, int maxNumTermsPerTopic, int maxNumTermsPerTimeSlot, string windowSize, int filterFlag, 
                                                  bool groupedZeroPadding)
        {
            windowSize = ParameterChecker.WindowSize(windowSize);
            entity = ParameterChecker.Entity(entity, windowSize);
            filterFlag = ParameterChecker.FilterFlagCheck(filterFlag);
            maxNumTopics = ParameterChecker.StrictlyPositiveNumber(maxNumTopics, 10);
            maxNumTermsPerTopic = ParameterChecker.PositiveNumber(maxNumTermsPerTopic, 0);
            maxNumTermsPerTimeSlot = ParameterChecker.PositiveNumber(maxNumTermsPerTimeSlot, 0);
            stepTimeSpan = ParameterChecker.StepTimeSpan(stepTimeSpan);
            dateTimeStart = ParameterChecker.DateRound(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRound(dateTimeEnd);
            groupedZeroPadding = ParameterChecker.Boolean(groupedZeroPadding);

            ParameterChecker.CheckTimeSlotNum(dateTimeStart, dateTimeEnd, stepTimeSpan, maxNumTopics);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTermsPerTopic*/", maxNumTermsPerTopic.ToString());
            strRpl.AddReplacement("/*#NumTermsPerTimeSlot*/", maxNumTermsPerTimeSlot.ToString());
            strRpl.AddReplacement("/*#NumTopics*/", maxNumTopics.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { (int)filterFlag, dateTimeStart, dateTimeEnd, (int) stepTimeSpan.TotalHours};
            string sqlTopicsOverTime = maxNumTermsPerTimeSlot==0 ? "TopicsOverTime.sql" : "TopicsOverTimeDetail.sql";

            List<SqlRow.TopicTimeSlot> topicTimeSlots = DataProvider.GetDataWithReplace<SqlRow.TopicTimeSlot>(sqlTopicsOverTime, strRpl, sqlParams);
            List<SqlRow.TopicTimeSlot> allTopicTimeSlots = DataProvider.GetDataWithReplace<SqlRow.TopicTimeSlot>("AllTopicsOverTime.sql", strRpl, sqlParams);
            Dictionary<int, SqlRow.TopicTimeSlot> allTopicTimeSlotsDict = allTopicTimeSlots.ToDictionary(t => t.TimeSlotGroup);

            if (!topicTimeSlots.Any())
                return new List<TopicOverTime>();
            
            List<SqlRow.TopicWeightedTerm> topicTerms = 
                maxNumTermsPerTopic==0
                    ? new List<SqlRow.TopicWeightedTerm>() 
                    : DataProvider.GetDataWithReplace<SqlRow.TopicWeightedTerm>("Topics.sql", strRpl, sqlParams);

            int minTimeSlotId = allTopicTimeSlots.Min(tts => tts.TimeSlotGroup);
            int maxTimeSlotId = allTopicTimeSlots.Max(tts => tts.TimeSlotGroup);
            var timeSlotsDef =
                Enumerable
                    .Range(minTimeSlotId, maxTimeSlotId - minTimeSlotId + 1)
                    .Select(timeSlotId =>
                            new
                                {
                                    TimeSlotId = timeSlotId,
                                    StartTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*timeSlotId),
                                    EndTimeDate = dateTimeStart + TimeSpan.FromTicks(stepTimeSpan.Ticks*(timeSlotId + 1)) - TimeSpan.FromMilliseconds(1)
                                })
                    .ToList();

            Dictionary<long, List<SqlRow.TopicWeightedTerm>> topicTermsDict = topicTerms
                .GroupBy(tt => tt.TopicId)
                .ToDictionary(ttGroup => ttGroup.Key, ttGroup => ttGroup.ToList());
            foreach (long topicId in topicTimeSlots.Select(tts=>tts.TopicId).Distinct().Where(topicId => !topicTermsDict.ContainsKey(topicId)))
            {
                topicTermsDict[topicId] = new List<SqlRow.TopicWeightedTerm>();
            }

            Dictionary<int, int> sumAllTopicNumDocDict = new Dictionary<int, int>();
            List<TopicOverTime> topicOverTime =
                topicTimeSlots
                .GroupBy(topic => new {topic.TopicId, topic.TopicNumDocs})
                .Select(topicGroup =>
                    {
                        Dictionary<int, List<SqlRow.TopicTimeSlot>> timeSlotDict = 
                            topicGroup
                            .GroupBy(tg => tg.TimeSlotGroup)
                            .ToDictionary(tg => tg.Key, tg => tg.ToList());

                        List<WeightedTerm> termList = topicTermsDict[topicGroup.Key.TopicId].Select(tt => new WeightedTerm() {Term = tt.Term, Weight = tt.Weight}).ToList();
                        if (termList.All(wt => wt.Term == null)) termList = new List<WeightedTerm>();

                        return new TopicOverTime()
                            {
                                TopicId = topicGroup.Key.TopicId,
                                NumDocs = topicGroup.Key.TopicNumDocs,
                                Terms = termList,
                                TimeSlots = timeSlotsDef
                                    .Select(timeSlotDef =>
                                        {
                                            List<SqlRow.TopicTimeSlot> timeSlotRows;
                                            SqlRow.TopicTimeSlot timeSlotFirst;

                                            timeSlotDict.TryGetValue(timeSlotDef.TimeSlotId, out timeSlotRows);
                                            if (timeSlotRows == null || timeSlotRows.Count == 0)
                                            {
                                                timeSlotFirst = new SqlRow.TopicTimeSlot();
                                            }
                                            else
                                            {
                                                timeSlotFirst = timeSlotRows.First();
                                                if (timeSlotFirst.StartTime < timeSlotDef.StartTimeDate || (timeSlotFirst.EndTime - TimeSpan.FromMilliseconds(1)) > timeSlotDef.EndTimeDate)
                                                    throw new Exception("Start time of a document inside a group has starting or ending time outside the group boundary!");
                                            }
                                            
                                            int sumNumDoc;
                                            sumAllTopicNumDocDict.TryGetValue(timeSlotDef.TimeSlotId, out sumNumDoc);
                                            sumAllTopicNumDocDict[timeSlotDef.TimeSlotId] = sumNumDoc + timeSlotFirst.TimeSlotNumDocs;
                                            
                                            return new TimeSlot()
                                                {
                                                    StartTimeDate = timeSlotDef.StartTimeDate,
                                                    EndTimeDate = timeSlotDef.EndTimeDate,
                                                    NumDocs = timeSlotFirst.TimeSlotNumDocs,
                                                    Terms = timeSlotFirst.TimeSlotNumDocs == 0 || timeSlotRows == null || timeSlotRows.All(ts => ts.Term == null)
                                                        ? null
                                                        : timeSlotRows
                                                            .Where(ts => ts.Term != null)
                                                            .Select(term =>
                                                                new WeightedTerm()
                                                                    {
                                                                        Term = term.Term,
                                                                        Weight = term.Weight
                                                                    })
                                                            .ToList()
                                                };
                                        })
                                    .ToList()
                            };
                    })
                .ToList();

            topicOverTime.Insert(
                0,                         
                new TopicOverTime
                        {
                            TopicId = -1,
                            NumDocs = allTopicTimeSlots.Sum(t => t.TopicNumDocs),
                            Terms = new List<WeightedTerm>(),
                            TimeSlots = timeSlotsDef
                                .Select(timeSlotDef => new TimeSlot
                                    {
                                        StartTimeDate = timeSlotDef.StartTimeDate,
                                        EndTimeDate = timeSlotDef.EndTimeDate,
                                        Terms = new List<WeightedTerm>(),
                                        NumDocs = (allTopicTimeSlotsDict.ContainsKey(timeSlotDef.TimeSlotId) ? allTopicTimeSlotsDict[timeSlotDef.TimeSlotId].TopicNumDocs : 0)
                                            - (sumAllTopicNumDocDict.ContainsKey(timeSlotDef.TimeSlotId) ? sumAllTopicNumDocDict[timeSlotDef.TimeSlotId] : 0)
                                    })
                                .ToList()
                        });

            if (groupedZeroPadding)
            {
                foreach (TopicOverTime tot in topicOverTime)
                {
                    List<TimeSlot> timeSlotsNew = new List<TimeSlot>();
                    TimeSlot timeSlotLast = null;
                    foreach (TimeSlot timeSlot in tot.TimeSlots)
                    {
                        if (timeSlotLast == null)
                        {
                            timeSlotsNew.Add(timeSlot);
                            timeSlotLast = timeSlot;
                        }
                        else
                        {
                            if (timeSlotLast.NumDocs == 0 & timeSlot.NumDocs == 0)
                                timeSlotLast.EndTimeDate = timeSlot.EndTimeDate;
                            else
                            {
                                timeSlotsNew.Add(timeSlot);                                
                                timeSlotLast = timeSlot;
                            }
                        }
                    }
                    tot.TimeSlots = timeSlotsNew;
                }
            }

            return topicOverTime;
        }


        //Dummy functions
        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public int GetFilterFlag(FilterFlag filterFlag)
        {
            return (int) filterFlag;
        }
        
        //Helper functions
        public StringReplacer StringReplacerGetDefaultBasic()
        {
            var strRpl = new StringReplacer();
            strRpl.AddReplacement("/*REM*/", "--");
            strRpl.AddReplacement("--ADD", "");

            return strRpl;
        }
        public StringReplacer StringReplacerGetDefault(string entity, string windowSize)
        {
            var strRpl = StringReplacerGetDefaultBasic();
            strRpl.AddReplacement("[AAPL_D_Terms]", string.Format("[Terms_{0}_{1}_1500]", entity, windowSize));
            strRpl.AddReplacement("[AAPL_D_Clusters]", string.Format("[Clusters_{0}_{1}_1500]", entity, windowSize));

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
