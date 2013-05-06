/*REM*/ DECLARE @entity VARCHAR(max) = 'BAC'
/*REM*/ DECLARE @windowSize VARCHAR(max) = 'W'
/*REM*/ DECLARE @dateTimeStart DATE = '2011-12-01'
/*REM*/ DECLARE @dateTimeEnd DATE = '2011-12-30'
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
--ADD DECLARE @dateTimeStart DATETIME = {2} 
--ADD DECLARE @dateTimeEnd DATETIME = {3}

 SELECT CAST(C.StartTime AS DATE) AS "Date",
        SUM(CAST(T.SentimentPos AS INT)) AS Positive, 
        SUM(CAST(T.SentimentNeg AS INT)) AS Negative,
        SUM(CAST(T.SentimentPosLowCfd AS INT)) AS NeutralPositiveBiased,
        SUM(CAST(T.SentimentNegLowCfd AS INT)) AS NeutralNegativeBiased
   FROM Entity E
        INNER JOIN Clusters C
                ON C.TableId = E.TableId
        INNER JOIN Tweets T          
                ON C.TableId = T.TableId AND C.Id = T.ClusterId
  WHERE E.Name = @entity
        AND E.WindowCode = @windowSize
        AND C.RecordState = 0
        AND CAST(C.StartTime AS DATE) >= @dateTimeStart
        AND CAST(DATEADD(MILLISECOND,-10,C.EndTime) AS DATE) <= @dateTimeEnd
        AND CAST(DATEADD(MILLISECOND,-10,T.EndTime) AS DATE) <= @dateTimeEnd
        AND T.RecordState = 0
  GROUP BY CAST(C.StartTime AS DATE)
  ORDER BY CAST(C.StartTime AS DATE) ASC