/*REM*/ DECLARE @entity VARCHAR(max) = 'MSFT'
/*REM*/ DECLARE @windowSize VARCHAR(max) = 'W'
/*REM*/ DECLARE @dateTimeStart DATE = '2012-01-01'
/*REM*/ DECLARE @dateTimeEnd DATE = '2012-03-30'
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
--ADD DECLARE @dateTimeStart DATETIME = {2} 
--ADD DECLARE @dateTimeEnd DATETIME = {3}

 SELECT ClusterId, C.TableId, CAST(C.StartTime AS DATE) AS "Date",
        SUM(C.SentimentPos) AS Cluster_SentimentPos, 
        SUM(C.SentimentNeg) AS Cluster_SentimentNeg,
        SUM(C.SentimentPosLowCfd) AS Cluster_SentimentPosLowCfd,
        SUM(C.SentimentNegLowCfd) AS Cluster_SentimentNegLowCfd,
        SUM(CAST(T.SentimentPos AS INT)) AS Tweet_SentimentPos, 
        SUM(CAST(T.SentimentNeg AS INT)) AS Tweet_SentimentNeg,
        SUM(CAST(T.SentimentPosLowCfd AS INT)) AS Tweet_SentimentPosLowCfd,
        SUM(CAST(T.SentimentNegLowCfd AS INT)) AS Tweet_SentimentNegLowCfd,
		count(distinct C.TableId) as tableids,
		count(distinct C.id) as clusters,
		count(distinct T.TweetId) as tweets_distict,
		count(T.TweetId) as tweets_all,
		sum(C.NumDocs) as tweets_clusternumdoc
   FROM Entity E
        INNER JOIN Clusters C
                ON C.TableId = E.TableId
        INNER JOIN Tweets T          
                ON C.TableId = T.TableId AND C.Id = T.ClusterId
  WHERE E.Name = @entity
        AND E.WindowCode = @windowSize
        AND C.RecordState = 0
        AND CAST(C.StartTime AS DATE) >= @dateTimeStart
        AND CAST(C.EndTime AS DATE) <= @dateTimeEnd
		AND CAST(T.EndTime AS DATE) <= @dateTimeEnd
		AND T.RecordState = 0
  GROUP BY CAST(C.StartTime AS DATE), ClusterId, C.TableId, TweetId
  ORDER BY CAST(C.StartTime AS DATE) ASC, ClusterId, C.TableId, TweetId
