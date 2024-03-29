﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Channels;
using System.ServiceModel.Web;
using System.Text;
using System.Web;
using Latino;
using Latino.Model;
using Latino.Visualization;


namespace TwitterMonitorDAL
{
    public enum Aggregate
    {
        Day,
        MA,
        Avg
    }
    public enum DataType
    {
        Sentiment
    }

    [ServiceContract]
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    public class TwitterMonitor
    {

        //******************************************************************************************

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
        public EntityInfoDetail EntityDetailOld(string entity, string windowSize)
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
            dateTimeStart = ParameterChecker.DateRoundToHour(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRoundToHour(dateTimeEnd);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTerms*/", maxNumTerms.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { entity, windowSize, (int)filterFlag, dateTimeStart, dateTimeEnd, maxNumTerms };

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
            dateTimeStart = ParameterChecker.DateRoundToHour(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRoundToHour(dateTimeEnd);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTermsPerTopic*/", maxNumTermsPerTopic.ToString());
            strRpl.AddReplacement("/*#NumTopics*/", maxNumTopics.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { entity, windowSize, (int)filterFlag, dateTimeStart, dateTimeEnd, maxNumTopics, maxNumTermsPerTopic };

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
            dateTimeStart = ParameterChecker.DateRoundToHour(dateTimeStart);
            dateTimeEnd = ParameterChecker.DateRoundToHour(dateTimeEnd);
            groupedZeroPadding = ParameterChecker.Boolean(groupedZeroPadding);

            ParameterChecker.CheckTimeSlotNum(dateTimeStart, dateTimeEnd, stepTimeSpan, maxNumTopics);

            StringReplacer strRpl = StringReplacerGetDefault(entity, windowSize);
            strRpl.AddReplacement("/*#NumTermsPerTopic*/", maxNumTermsPerTopic.ToString());
            strRpl.AddReplacement("/*#NumTermsPerTimeSlot*/", maxNumTermsPerTimeSlot.ToString());
            strRpl.AddReplacement("/*#NumTopics*/", maxNumTopics.ToString());
            StringReplacerAddFilterFlag(strRpl, (FilterFlag)filterFlag);

            var sqlParams = new object[] { entity, windowSize, (int)filterFlag, dateTimeStart, dateTimeEnd, (int)stepTimeSpan.TotalHours, maxNumTopics, maxNumTermsPerTimeSlot };
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

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<ContentMapTopic> ContentMap(string entity, DateTime dateTimeStart, DateTime dateTimeEnd, int maxNumTermsPerTopic,
                                  int maxNumTopics, string windowSize, int filterFlag)
        {
            

            Dictionary<string, int> dims = new Dictionary<string, int>();
            ArrayList<string> names = new ArrayList<string>();
            int lastIdx = 0;
            List<Topic> topics = Topics(entity, dateTimeStart, dateTimeEnd, maxNumTermsPerTopic, maxNumTopics, windowSize, filterFlag);
            // compute hierarchy
            UnlabeledDataset<SparseVector<double>> ds = new UnlabeledDataset<SparseVector<double>>();
            foreach (Topic topic in topics)
            {
                SparseVector<double> vec = new SparseVector<double>();
                names.Add("");
                int i = 0;
                int n = 5;
                foreach (WeightedTerm item in topic.Terms)
                {
                    int idx;
                    if (!dims.TryGetValue(item.Term, out idx)) { idx = lastIdx; dims.Add(item.Term, lastIdx++); }
                    vec[idx] = item.Weight;
                    if (i < n) { names.Last += item.Term.ToLower() + ", "; }
                    i++;
                }
                ModelUtils.TryNrmVecL2(vec);
                ds.Add(vec);
            }
            //Clustering cl = new Clustering();
            //cl.Cluster(ds);
            // compute layout
            StressMajorizationLayout layalgo = new StressMajorizationLayout(ds.Count, new Dist(ds));
            layalgo.MinDiff = 0.0005;
            Vector2D[] layout = layalgo.ComputeLayout();

            List<ContentMapTopic> contentMapTopics = new List<ContentMapTopic>();
            for (int i = 0; i < layout.Length; i++)
            {
                List<WeightedTerm> terms = topics[i].Terms.Select(
                    term => new WeightedTerm()
                    {
                        Term = term.Term,
                        Weight = term.Weight
                    }).ToList();

                contentMapTopics.Add(new ContentMapTopic()
                {
                    TopicId = topics[i].TopicId,
                    NumDocs = topics[i].NumDocs,
                    Terms = terms,
                    PosX = layout[i].X,
                    PosY = layout[i].Y
                });
            }
            return contentMapTopics;
        }

        //******************************************************************************************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<SentimentTimelineDay> GetSentimentTimeline(string stock, DateTime from, DateTime to, DateTime date, int days)
        {
            string entity = ParameterChecker.Stock(stock);
            string windowSize = ParameterChecker.FirstWindowSize(entity);
            entity = ParameterChecker.Entity(entity, windowSize);

            from = ParameterChecker.DateRoundToDayLeaveMin(from);
            to = ParameterChecker.DateRoundToDayLeaveMin(to);
            date = ParameterChecker.DateRoundToDayLeaveMin(date);
            days = ParameterChecker.StrictlyPositiveNumber(days, 0);

            StringReplacer strRpl = StringReplacerGetDefaultBasic();

            // first type of invocation (from & to), nothing to be done
            if (from != DateTime.MinValue && to != DateTime.MinValue) {} 
                // second type of invocation (days)
            else if (days > 0)
            {
                to = ParameterChecker.DateRoundToDayLeaveMin(DateTime.Now);
                from = to - new TimeSpan(days-1, 0, 0, 0);
            }
                // third type of invocation (date)
            else if (date != DateTime.MinValue)
            {
                to = ParameterChecker.DateRoundToDayLeaveMin(date);
                from = to;
            } 
                // else throw exception
            else
            {
                throw new WebFaultException<string>(
                    string.Format("One of the following sets of parameters must be set: (stock, from, to) or (stock, date) or (stock, days)!"),
                    HttpStatusCode.NotAcceptable
                    );
            }

            var sqlParams = new object[] {entity, windowSize, from, to };

            List<SqlRow.SentimentTimelineDay> sentimentTS = DataProvider.GetDataWithReplace<SqlRow.SentimentTimelineDay>("SentimentTimeline.sql", strRpl, sqlParams);

            if (!sentimentTS.Any())
                return new List<SentimentTimelineDay>();

            Dictionary<DateTime, SqlRow.SentimentTimelineDay> sentimentTSDict = sentimentTS.ToDictionary(sd => sd.Date);

            DateTime actualTo = sentimentTS.Max(sd => sd.Date);
            int daySpan = (int)Math.Round((actualTo - from).TotalDays);

            return Enumerable
                .Range(0, daySpan + 1)
                .Select(dateId =>
                {
                    DateTime dayDate = from.AddDays(dateId);
                    SentimentData data;
                    if (sentimentTSDict.ContainsKey(dayDate))
                    {
                        SqlRow.SentimentTimelineDay existData = sentimentTSDict[dayDate];
                        data = new SentimentData
                        {
                            Positive = existData.Positive,
                            Negative = existData.Negative,
                            NeutralPositiveBiased = existData.NeutralPositiveBiased,
                            NeutralNegativeBiased = existData.NeutralNegativeBiased,
                        };
                    }
                    else
                    {
                        data = new SentimentData();
                    }
                    return new SentimentTimelineDay
                    {
                        DateDate = dayDate,
                        SentimentValue = data
                    };
                })
                .ToList();
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<TweetSentiment> GetTweets(string stock, DateTime from, DateTime to, DateTime date, int days)
        {
            string entity = ParameterChecker.Stock(stock);
            string windowSize = ParameterChecker.FirstWindowSize(entity);
            entity = ParameterChecker.Entity(entity, windowSize);

            from = ParameterChecker.DateRoundToDayLeaveMin(from);
            to = ParameterChecker.DateRoundToDayLeaveMin(to);
            date = ParameterChecker.DateRoundToDayLeaveMin(date);
            days = ParameterChecker.StrictlyPositiveNumber(days, 0);

            StringReplacer strRpl = StringReplacerGetDefaultBasic();

            // first type of invocation (from & to), nothing to be done
            if (from != DateTime.MinValue && to != DateTime.MinValue)
            {
                if (from != to)
                    throw new WebFaultException<string>(
                        string.Format("Currently, max allowed time span for twitter retrieval is one day - meaning that 'from' and 'to' parameters must point to the same date!"),
                        HttpStatusCode.NotAcceptable);
            }
                // second type of invocation (days)
            else if (days > 0)
            {
                if (days > 1)
                    throw new WebFaultException<string>(
                        string.Format("Currently, max allowed time span for twitter retrieval is one day - meaning that 'days' parameter shouldnt be greater than 1!"),
                        HttpStatusCode.NotAcceptable);

                to = ParameterChecker.DateRoundToDayLeaveMin(DateTime.Now);
                from = to - new TimeSpan(days - 1, 0, 0, 0);
            }
                // third type of invocation (date)
            else if (date != DateTime.MinValue)
            {
                to = ParameterChecker.DateRoundToDayLeaveMin(date);
                from = to;
            }
                // else throw exception
            else
            {
                throw new WebFaultException<string>(
                    string.Format("One of the following sets of parameters must be set: (stock, from, to) or (stock, date) or (stock, days)!"),
                    HttpStatusCode.NotAcceptable
                    );
            }

            var sqlParams = new object[] { entity, windowSize, from, to };

            List<SqlRow.TweetSentiment> sentiTweets = DataProvider.GetDataWithReplace<SqlRow.TweetSentiment>("GetTweetsSentiment.sql", strRpl, sqlParams);

            return sentiTweets.Select(t => new TweetSentiment
            {
                DateDate = t.Date??DateTime.MinValue,
                Text = t.Text,
                UserName = t.UserName,
                SentimentScore = t.SentimentScore
            }).ToList();
        }

        [OperationContract]
        [WebGet]
        public Stream DisplayTweets(string stock, DateTime from, DateTime to, DateTime date, int days, string css)
        {
            List<TweetSentiment> tweets = GetTweets(stock, from, to, date, days);

            string html = @"<!DOCTYPE html PUBLIC ""-//W3C//DTD XHTML 1.0 Transitional//EN"" ""http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"">
            <html xmlns=""http://www.w3.org/1999/xhtml"">
            <head>
                <title><%= mTitle %></title>
                <script type=""text/javascript"" src=""http://first.ijs.si/demos/common/js/standardista-table-sorting/common.js""></script>
                <script type=""text/javascript"" src=""http://first.ijs.si/demos/common/js/standardista-table-sorting/css.js""></script>
                <script type=""text/javascript"" src=""http://first.ijs.si/demos/common/js/standardista-table-sorting/standardista-table-sorting.js""></script>
                <link href=""<%= mCssUrl %>"" rel=""stylesheet"" type=""text/css""/>
            </head>
            <body>    
                <center>
                <h1><%= mTitle %></h1>
                <table class=""sortable"">
                <thead>
                <tr>
                <th class=""time"">Time</th>
                <th class=""user"">User</th>
                <th class=""tweet"">Tweet</th>
                <th class=""sentiment"">Sentiment</th>
                </tr>  
                </thead>
                <tbody>
                    <%= mTBody %>
                </tbody>     
                </table>
                </center>
            </body>
            </html>
            ";

            StringBuilder sbTBody = new StringBuilder();
            const double THRESH = 0.2;
            int i = 0;
            foreach (TweetSentiment row in tweets)
            {

                double sentimentDispl = Convert.ToDouble((row.SentimentScore).ToString("0.00"));
                string sentimentClass;
                if (row.SentimentScore > THRESH)
                {
                    sentimentClass = "positive";
                }
                else if (row.SentimentScore < -THRESH)
                {
                    sentimentClass = "negative";
                }
                else
                {
                    sentimentClass = "neutral";
                    if (Math.Abs(sentimentDispl) == THRESH)
                    {
                        sentimentDispl -= Math.Sign(sentimentDispl)*0.01;
                    } // adjust border value (for sorting)
                }
                sbTBody.Append(string.Format(@"
                                            <tr class=""{0} {5}"">
                                            <td class=""time {5}"">{1:yyyy-MM-dd HH:mm:ss}</td>
                                            <td class=""user {5}"">{2}</td>
                                            <td class=""tweet {5}"">{3}</td>
                                            <td class=""sentiment {5}"">{4:0.00}</td>
                                            </tr>",
                    ++i%2 == 0 ? "odd" : "",
                    row.DateDate,
                    HttpUtility.HtmlEncode((string)row.UserName),
                    HttpUtility.HtmlEncode((string)row.Text),
                    sentimentDispl,
                    sentimentClass));
            }

            string title = stock.TrimStart('$') + " on " + tweets.First().DateDate.ToString("yyyy-MM-dd");
            string finalHtml =
                html
                .Replace("<%= mTitle %>", title)
                .Replace("<%= mCssUrl %>", css)
                .Replace("<%= mTBody %>", sbTBody.ToString());

            WebOperationContext.Current.OutgoingResponse.ContentType = "text/html";

            return new MemoryStream(Encoding.UTF8.GetBytes(finalHtml));
        }

        //************* Common API ********************************************************************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<Entity> FindEntity(string query)
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            StringReplacerCommentNamedRemove(strRpl, "uri");
            var sqlParams = new object[] { Guid.Empty, query };
            
            List<SqlRow.Entity> entities = DataProvider.GetDataWithReplace<SqlRow.Entity>("Entity.sql", strRpl, sqlParams);

            return entities
                .Select(ent => new Entity
                {
                    Id = ent.EntityUri,
                    Description = ent.EntityLabel,
                    Features = ent.Features
                })
                .ToList();
        }

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<EntityDetail> EntityDetail(string entity)
        {
            if (string.IsNullOrWhiteSpace(entity)) return new List<EntityDetail>();

            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            Guid entityGuid = ParameterChecker.EntityGuid(entity);

            var sqlParams = new object[] { entityGuid.ToString() };

            List<SqlRow.Entity> volumeTimeSeries = DataProvider.GetDataWithReplace<SqlRow.Entity>("EntityDetailNew.sql", strRpl, sqlParams);

            return volumeTimeSeries.Select(d => new EntityDetail()
            {
                Id = d.EntityUri,
                Description = d.EntityLabel,
                NumDocuments = d.NumDocuments,
                NumOccurrences = d.NumOccurrences,
                DataStartTimeDate = d.DataStartTime ?? DateTime.MinValue,
                DataEndTimeDate = d.DataEndTime ?? DateTime.MinValue,
                Features = d.Features
            }).ToList();

        }

        // ************ Common functions *******************

        private static TDayData[] FillMissingDates<TSqlRowType, TDayData>(DateTime from, int days, IEnumerable<TSqlRowType> volumeTimeSeries, Func<TSqlRowType, TDayData> createFromExisting, Func<TSqlRowType, DateTime> date)
            where TDayData : DayData, new()
        {
            // important: imput 0 values if they are absent for specific days !!!!!
            List<TSqlRowType> volumeTimeSeriesOrdered = volumeTimeSeries.OrderBy(date).ToList();

            int j = 0;
            TDayData[] returnArray = new TDayData[days];
            for (int i = 0; i < days; i++)
            {
                if (j < volumeTimeSeriesOrdered.Count && date(volumeTimeSeriesOrdered[j]) == from.AddDays(i).Date)
                {
                    returnArray[i] = createFromExisting(volumeTimeSeriesOrdered[j]);
                    j++;
                }
                else
                {
                    returnArray[i] = new TDayData() { DateDate = from.AddDays(i) };
                }
            }
            return returnArray;
        }

        // ************ Volume *******************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<DayVolume> Volume(string entity, DateTime from, DateTime to, int days, string data, string aggregate, int maWindow)
        {
            if (string.IsNullOrWhiteSpace(entity)) return new List<DayVolume>();

            Guid entityGuid = ParameterChecker.EntityGuid(entity);
            ParameterChecker.TimeSpan(ref from, ref to, ref days);
            DataType dataEnum = ParameterChecker.DataType(data, DataType.Sentiment);
            Aggregate aggEnum = ParameterChecker.Aggregate(aggregate, Aggregate.Day);
            maWindow = ParameterChecker.StrictlyPositiveNumber(maWindow, 0);
            if (aggEnum == Aggregate.MA)
            {
                to = to.AddDays(maWindow);
                days += maWindow;
            }

            List<DayVolume> returnArray = GetVolume(entityGuid.ToString(), from, to, days);
            switch (aggEnum)
            {
                case Aggregate.Day:
                    return returnArray;
                case Aggregate.MA:
                    return GetVolumeMovingAvg(returnArray, maWindow);
                case Aggregate.Avg:
                    return GetVolumeAvg(returnArray);
                default:
                    throw new ArgumentOutOfRangeException();
            }

        }

        private List<DayVolume> GetVolume(string entity, DateTime from, DateTime to, int days)
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            var sqlParams = new object[] { entity, from, to, days, 0 };
            List<SqlRow.DayVolume> volumeTimeSeries = DataProvider.GetDataWithReplace<SqlRow.DayVolume>("DayVolumeSentiment.sql", strRpl, sqlParams);

            var returnArray = FillMissingDates(from, days, volumeTimeSeries, dv => new DayVolume() { DateDate = dv.Date, Volume = dv.Volume }, dv => dv.Date);
            return returnArray.ToList();
        }
        public List<DayVolume> GetVolumeMovingAvg(List<DayVolume> ds, int maWindow)
        {
            int period = ds.Count - maWindow - 1;  // number of days between dateEnd and dateStart 

            List<DayVolume> result = new List<DayVolume>();
            for (int i = maWindow - 1; i < period + 1; i++)
            {
                double avg = 0;
                for (int j = 0; j < maWindow; j++)
                {
                    avg = avg + ds[i - j].Volume;
                }
                avg = avg / maWindow;

                DayVolume avgds = new DayVolume { DateDate = ds[i].DateDate, Volume = avg };
                result.Add(avgds);
            }

            return result;
        }
        public List<DayVolume> GetVolumeAvg(List<DayVolume> ds)
        {
            return new List<DayVolume>(new[]
                {
                    new DayVolume
                        {
                            DateDate = ds.Min(d => d.DateDate),
                            Volume = ds.Average(d => d.Volume)
                        }
                });
        }

        // ************ Index *******************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<DayIndex> Index(string entity, DateTime from, DateTime to, int days, string data, string aggregate, int maWindow, bool normalize)
        {
            if (string.IsNullOrWhiteSpace(entity)) return new List<DayIndex>();

            Guid entityGuid = ParameterChecker.EntityGuid(entity);
            ParameterChecker.TimeSpan(ref from, ref to, ref days);
            DataType dataEnum = ParameterChecker.DataType(data, DataType.Sentiment);
            Aggregate aggEnum = ParameterChecker.Aggregate(aggregate, Aggregate.Day);
            maWindow = ParameterChecker.StrictlyPositiveNumber(maWindow, 0);
            if (aggEnum == Aggregate.MA)
            {
                to = to.AddDays(maWindow);
                days += maWindow;
            }

            List<DayIndex> returnArray = GetIndex(entityGuid.ToString(), from, to, days, dataEnum, normalize);
            switch (aggEnum)
            {
                case Aggregate.Day:
                    return returnArray;
                case Aggregate.MA:
                    return GetIndexMovingAvg(returnArray, maWindow);
                case Aggregate.Avg:
                    return GetIndexAvg(returnArray);
                default:
                    throw new ArgumentOutOfRangeException();
            }

        }

        private List<DayIndex> GetIndex(string entity, DateTime from, DateTime to, int days, DataType dataType, bool normalize)
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            var sqlParams = new object[] { entity, from, to, days, normalize };
            List<SqlRow.DayIndex> indexTimeSeries = DataProvider.GetDataWithReplace<SqlRow.DayIndex>("DayVolumeSentiment.sql", strRpl, sqlParams);

            var returnArray = FillMissingDates(from, days, indexTimeSeries, dv => new DayIndex() { DateDate = dv.Date, Index = dv.Index }, dv => dv.Date);
            if (normalize)
                foreach (DayIndex di in returnArray)
                {
                    if (di.Index > 1) di.Index = 1;
                    if (di.Index < -1) di.Index = -1;
                }
            return returnArray.ToList();
        }
        public List<DayIndex> GetIndexMovingAvg(List<DayIndex> ds, int maWindow)
        {
            int period = ds.Count - maWindow - 1;  // number of days between dateEnd and dateStart 

            List<DayIndex> result = new List<DayIndex>();
            for (int i = maWindow - 1; i < period + 1; i++)
            {
                double avg = 0;
                for (int j = 0; j < maWindow; j++)
                {
                    avg = avg + ds[i - j].Index;
                }
                avg = avg / maWindow;

                DayIndex avgds = new DayIndex { DateDate = ds[i].DateDate, Index = avg };
                result.Add(avgds);
            }

            return result;
        }
        public List<DayIndex> GetIndexAvg(List<DayIndex> ds)
        {
            return new List<DayIndex>(new[]
                {
                    new DayIndex
                        {
                            DateDate = ds.Min(d => d.DateDate),
                            Index = ds.Average(d => d.Index)
                        }
                });
        }

        // ************ VolumeAndIndex *******************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<DayVolumeIndex> VolumeAndIndex(string entity, DateTime from, DateTime to, int days, string data, string aggregate, int maWindow, bool normalize)
        {
            if (string.IsNullOrWhiteSpace(entity)) return new List<DayVolumeIndex>();

            Guid entityGuid = ParameterChecker.EntityGuid(entity);
            ParameterChecker.TimeSpan(ref from, ref to, ref days);
            DataType dataEnum = ParameterChecker.DataType(data, DataType.Sentiment);
            Aggregate aggEnum = ParameterChecker.Aggregate(aggregate, Aggregate.Day);
            maWindow = ParameterChecker.StrictlyPositiveNumber(maWindow, 0);
            if (aggEnum == Aggregate.MA)
            {
                to = to.AddDays(maWindow);
                days += maWindow;
            }

            List<DayVolumeIndex> returnArray = GetVolumeIndex(entityGuid.ToString(), from, to, days, dataEnum, normalize);
            switch (aggEnum)
            {
                case Aggregate.Day:
                    return returnArray;
                case Aggregate.MA:
                    return GetVolumeIndexMovingAvg(returnArray, maWindow);
                case Aggregate.Avg:
                    return GetVolumeIndexAvg(returnArray);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private List<DayVolumeIndex> GetVolumeIndex(string entity, DateTime from, DateTime to, int days, DataType dataType, bool normalize)
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            var sqlParams = new object[] { entity, from, to, days, normalize };
            List<SqlRow.DayVolumeIndex> volumeIndexTimeSeries = DataProvider.GetDataWithReplace<SqlRow.DayVolumeIndex>("DayVolumeSentiment.sql", strRpl, sqlParams);

            var returnArray = FillMissingDates(
                from, 
                days, 
                volumeIndexTimeSeries,
                dv => new DayVolumeIndex()
                {
                    DateDate = dv.Date,
                    Volume = dv.Volume,
                    Index = dv.Index
                },
                dv => dv.Date);
            if (normalize)
                foreach (DayVolumeIndex di in returnArray)
                {
                    if (di.Index > 1) di.Index = 1;
                    if (di.Index < -1) di.Index = -1;
                }

            return returnArray.ToList();

        }
        public List<DayVolumeIndex> GetVolumeIndexMovingAvg(List<DayVolumeIndex> ds, int maWindow)
        {
            int period = ds.Count - maWindow - 1;  // number of days between dateEnd and dateStart 

            List<DayVolumeIndex> result = new List<DayVolumeIndex>();
            for (int i = maWindow - 1; i < period + 1; i++)
            {
                DayVolumeIndex avg = new DayVolumeIndex();
                avg.DateDate = ds[i].DateDate;
                for (int j = 0; j < maWindow; j++)
                {
                    avg.Volume += ds[i - j].Volume / maWindow;
                    avg.Index += ds[i - j].Index / maWindow;
                }
                result.Add(avg);
            }

            return result;
        }
        public List<DayVolumeIndex> GetVolumeIndexAvg(List<DayVolumeIndex> ds)
        {
            return new List<DayVolumeIndex>(new[]
                {
                    new DayVolumeIndex
                        {
                            DateDate = ds.Min(d => d.DateDate),
                            Volume = ds.Average(d => d.Volume),
                            Index = ds.Average(d => d.Index)
                        }
                });
        }

        // ************ IndexClasses *******************

        [OperationContract]
        [WebGet(ResponseFormat = WebMessageFormat.Json)]
        public List<DayIndexClasses> IndexClasses(string entity, DateTime from, DateTime to, int days, string data, string aggregate, int maWindow)
        {
            if (string.IsNullOrWhiteSpace(entity)) return new List<DayIndexClasses>();

            Guid entityGuid = ParameterChecker.EntityGuid(entity);
            ParameterChecker.TimeSpan(ref from, ref to, ref days);
            DataType dataEnum = ParameterChecker.DataType(data, DataType.Sentiment);
            Aggregate aggEnum = ParameterChecker.Aggregate(aggregate, Aggregate.Day);
            maWindow = ParameterChecker.StrictlyPositiveNumber(maWindow, 0);
            if (aggEnum == Aggregate.MA)
            {
                to = to.AddDays(maWindow);
                days += maWindow;
            }

            List<DayIndexClasses> returnArray = GetIndexClasses(entityGuid.ToString(), from, to, days, dataEnum);
            switch (aggEnum)
            {
                case Aggregate.Day:
                    return returnArray;
                case Aggregate.MA:
                    return GetIndexClassesMovingAvg(returnArray, maWindow);
                case Aggregate.Avg:
                    return GetIndexClassesAvg(returnArray);
                default:
                    throw new ArgumentOutOfRangeException();
            }

        }

        private List<DayIndexClasses> GetIndexClasses(string entity, DateTime from, DateTime to, int days, DataType dataType)
        {
            StringReplacer strRpl = StringReplacerGetDefaultBasic();
            var sqlParams = new object[] { entity, from, to, days };
            List<SqlRow.DayIndexClasses> indexClassesTimeSeries = DataProvider.GetDataWithReplace<SqlRow.DayIndexClasses>("DaySentimentClasses.sql", strRpl, sqlParams);

            var returnArray = FillMissingDates(
                from, 
                days, 
                indexClassesTimeSeries,
                dv => new DayIndexClasses()
                {
                    DateDate = dv.Date,
                    Positives = dv.Positives,
                    PosNeutrals = dv.PosNeutrals,
                    NegNeutrals = dv.NegNeutrals,
                    Negatives = dv.Negatives
                },
                dv => dv.Date);
            return returnArray.ToList();
        }
        public List<DayIndexClasses> GetIndexClassesMovingAvg(List<DayIndexClasses> ds, int maWindow)
        {
            int period = ds.Count - maWindow - 1;  // number of days between dateEnd and dateStart 

            List<DayIndexClasses> result = new List<DayIndexClasses>();
            for (int i = maWindow - 1; i < period + 1; i++)
            {
                DayIndexClasses avg = new DayIndexClasses();
                avg.DateDate = ds[i].DateDate;
                for (int j = 0; j < maWindow; j++)
                {
                    avg.Positives += ds[i - j].Positives / maWindow;
                    avg.PosNeutrals += ds[i - j].PosNeutrals / maWindow;
                    avg.NegNeutrals += ds[i - j].NegNeutrals / maWindow;
                    avg.Negatives += ds[i - j].Negatives / maWindow;
                }
                result.Add(avg);
            }

            return result;
        }
        public List<DayIndexClasses> GetIndexClassesAvg(List<DayIndexClasses> ds)
        {
            return new List<DayIndexClasses>(new[]
                {
                    new DayIndexClasses
                        {
                            DateDate = ds.Min(d => d.DateDate),
                            Positives = ds.Average(d => d.Positives),
                            PosNeutrals = ds.Average(d => d.PosNeutrals),
                            NegNeutrals = ds.Average(d => d.NegNeutrals),
                            Negatives = ds.Average(d => d.Negatives)
                        }
                });
        }

        
        //******************************************************************************************
        
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

            int maxParameters = 20;
            for (int i = 0; i < maxParameters; i++)
                strRpl.AddReplacement("{" + i + "}", "#$#ENC$" + i + "#$#ENC$");
            strRpl.AddReplacement("{", "{{");
            strRpl.AddReplacement("}", "}}");
            for (int i = 0; i < maxParameters; i++)
                strRpl.AddReplacement("#$#ENC$" + i + "#$#ENC$", "{" + i + "}");

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
        public StringReplacer StringReplacerCommentNamedRemove(StringReplacer strRpl, string name)
        {
            strRpl.AddReplacement(string.Format("/*REM {0}*/", name), "--");
            return strRpl;
        }
    }

    class Dist : IDistance<int>
    {
        UnlabeledDataset<SparseVector<double>> mDs;

        public Dist(UnlabeledDataset<SparseVector<double>> ds)
        {
            mDs = ds;
        }

        public double GetDistance(int a, int b)
        {
            return 1.0 - DotProductSimilarity.Instance.GetSimilarity(mDs[a], mDs[b]);
        }

        public void Save(BinarySerializer writer)
        {
            throw new NotImplementedException();
        }
    }
}
