/*REM*/ DECLARE @entity VARCHAR(max) = 'BAC'
/*REM*/ DECLARE @windowSize VARCHAR(max) = 'W'
/*REM*/ DECLARE @dateTimeStart DATE = '2012-12-01'
/*REM*/ DECLARE @dateTimeEnd DATE = '2012-12-01'
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
--ADD DECLARE @dateTimeStart DATETIME = {2} 
--ADD DECLARE @dateTimeEnd DATETIME = {3}

 SELECT T.TweetId                      AS TweetId,
        TDB.CreatedAt                  AS "Date",
        CAST(TDB.Text AS VARCHAR(MAX)) AS Text,
		TDB.UserName                   AS UserName,
		T.Sentiment                    AS SentimentScore
   FROM Entity E
        INNER JOIN Clusters C
                ON C.TableId = E.TableId
        INNER JOIN Tweets T          
                ON C.TableId = T.TableId AND C.Id = T.ClusterId
		LEFT OUTER JOIN TwitterStockDacq..Tweets TDB 
		        ON T.TweetId = TDB.Id
  WHERE E.Name = @entity
        AND E.WindowCode = @windowSize
        AND C.RecordState = 0
        AND CAST(C.StartTime AS DATE) >= @dateTimeStart
        AND CAST(DATEADD(MILLISECOND,-10,C.EndTime) AS DATE) <= @dateTimeEnd
        AND CAST(DATEADD(MILLISECOND,-10,T.EndTime) AS DATE) <= @dateTimeEnd
        AND T.RecordState = 0
  ORDER BY T.TweetId
  