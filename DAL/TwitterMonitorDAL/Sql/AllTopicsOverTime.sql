/*REM*/ DECLARE @filterFlag INT = 1
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T01:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-02-01T00:00:00' 
/*REM*/ DECLARE @stepTimeSpan INT = 1
--ADD DECLARE @filterFlag INT = {0}
--ADD DECLARE @dateTimeStart DATETIME = {1} 
--ADD DECLARE @dateTimeEnd DATETIME = {2}
--ADD DECLARE @stepTimeSpan INT = {3}

  SELECT Sum(NumDocs) TopicNumDocs,
         DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan TimeSlotGroup,
         Min(StartTime) StartTime,
         Max(EndTime) EndTime
    FROM [Clusters_AAPL_W_1500] Clusters
   WHERE Clusters.StartTime >= @dateTimeStart AND
         Clusters.EndTime <= @dateTimeEnd
         and Clusters.RecordState = 0
GROUP BY DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan
ORDER BY DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan
