/*REM*/ DECLARE @entity VARCHAR(max) = 'MSFT'
/*REM*/ DECLARE @windowSize VARCHAR(max) = 'W'
/*REM*/ DECLARE @filterFlag INT = 1         --Not used in this SQL
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T00:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-03-30T00:00:00'
/*REM*/ DECLARE @stepTimeSpan INT = 24*7
/*REM*/ DECLARE @numTopics INT = 1
/*REM*/ DECLARE @numTermsTimeSlot INT = 5   --Not used in this SQL
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
--ADD DECLARE @filterFlag INT = {2}         --Not used in this SQL
--ADD DECLARE @dateTimeStart DATETIME = {3} 
--ADD DECLARE @dateTimeEnd DATETIME = {4}
--ADD DECLARE @stepTimeSpan INT = {5}
--ADD DECLARE @numTopics INT = {6}
--ADD DECLARE @numTermsTimeSlot INT = {7}   --Not used in this SQL

 SELECT Topic AS TopicId,
        Topic.NumDocs AS TopicNumDocs,
	    SUM(C.NumDocs) AS TimeSlotNumDocs,
        DATEDIFF(hour, @dateTimeStart, C.StartTime)/@stepTimeSpan AS TimeSlotGroup,
        MIN(C.StartTime) AS StartTime,
        MAX(C.EndTime) AS EndTime
   FROM Entity E
        INNER JOIN Clusters C
                ON C.TableId = E.TableId
        INNER JOIN (
         SELECT TOP (@numTopics)
                C.Topic AS TopicId,
                SUM(C.NumDocs) AS NumDocs
           FROM Entity E
                INNER JOIN Clusters C
                        ON C.TableId = E.TableId
          WHERE E.Name = @entity
                AND E.WindowCode = @windowSize
                AND C.RecordState = 0
                AND C.StartTime >= @dateTimeStart
                AND C.EndTime <= @dateTimeEnd
          GROUP BY C.Topic
          ORDER BY SUM(C.NumDocs) DESC
         ) AS Topic          
                ON C.Topic = Topic.TopicId
  WHERE E.Name = @entity
        AND E.WindowCode = @windowSize
        AND C.RecordState = 0
        AND C.StartTime >= @dateTimeStart
        AND C.EndTime <= @dateTimeEnd
  GROUP BY C.Topic,
           Topic.NumDocs,
           DATEDIFF(hour, @dateTimeStart, C.StartTime)/@stepTimeSpan
  ORDER BY C.Topic,
           DATEDIFF(hour, @dateTimeStart, C.StartTime)/@stepTimeSpan
