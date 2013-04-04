/*REM*/ DECLARE @filterFlag INT = 1
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T01:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-02-01T00:00:00' 
/*REM*/ DECLARE @stepTimeSpan INT = 3
--ADD DECLARE @filterFlag INT = {0}
--ADD DECLARE @dateTimeStart DATETIME = {1} 
--ADD DECLARE @dateTimeEnd DATETIME = {2}
--ADD DECLARE @stepTimeSpan INT = {3}

  SELECT Topic TopicId,
         Topic.NumDocs TopicNumDocs,
         Sum(Clusters.NumDocs) TimeSlotNumDocs,
         DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan TimeSlotGroup,
         Min(StartTime) StartTime,
         Max(EndTime) EndTime
    FROM [AAPL_D_Clusters] Clusters
         INNER JOIN
         (    SELECT /*REM*/ TOP 10
                     --ADD   TOP /*#NumTopics*/
                      Topic TopicId,
                      Sum(NumDocs) NumDocs
                 FROM [AAPL_D_Clusters] Clusters
                WHERE Clusters.StartTime >= @dateTimeStart AND
                      Clusters.EndTime <= @dateTimeEnd
             GROUP BY Topic
             ORDER BY Sum(NumDocs) DESC
         ) AS Topic
         ON (Topic = Topic.TopicId)
   WHERE Clusters.StartTime >= @dateTimeStart AND
         Clusters.EndTime <= @dateTimeEnd
		 and Clusters.RecordState = 0
GROUP BY Topic,
         Topic.NumDocs,
         DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan
ORDER BY Topic,
         DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan
