/*REM*/ DECLARE @entity VARCHAR(max) = 'MSFT'
/*REM*/ DECLARE @windowSize VARCHAR(max) = 'W'
/*REM*/ DECLARE @filterFlag INT = 1
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T00:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-03-30T00:00:00'
/*REM*/ DECLARE @numTopics INT = 20
/*REM*/ DECLARE @numTerms INT = 10
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
--ADD DECLARE @filterFlag INT = {2}
--ADD DECLARE @dateTimeStart DATETIME = {3} 
--ADD DECLARE @dateTimeEnd DATETIME = {4}
--ADD DECLARE @numTopics INT = {5}
--ADD DECLARE @numTerms INT = {6}

 SELECT Topic.TopicId,
        Topic.NumDocs,
        Terms.Term,
        Terms.Weight
   FROM (SELECT TOP (@numTopics)
                C.Topic TopicId,
                Sum(C.NumDocs) NumDocs
           FROM Entity E
                INNER JOIN Clusters C
                        ON C.TableId = E.TableId
          WHERE E.Name = @entity
                AND E.WindowCode = @windowSize
                AND C.RecordState = 0
                AND C.StartTime >= @dateTimeStart
                AND C.EndTime <= @dateTimeEnd
          GROUP BY C.Topic
          ORDER BY Sum(C.NumDocs) DESC
         ) AS Topic
         CROSS APPLY
         (SELECT TOP (@numTerms)
                 MIN(T.MostFrequentForm) AS Term,
                 SUM(T.TFIDF)          AS Weight
            FROM Entity E
                 INNER JOIN Clusters C
                         ON C.TableId = E.TableId
                 INNER JOIN Terms T
                         ON T.TableId = E.TableId 
                            AND T.ClusterId = C.Id
           WHERE E.Name = @entity
                 AND E.WindowCode = @windowSize
                 AND C.RecordState = 0
                 AND C.StartTime >= @dateTimeStart
                 AND C.EndTime <= @dateTimeEnd
                 AND C.Topic = Topic.TopicId
                 AND T.RecordState = 0
                 AND ( 0=1
                     /*REM TermUnigram*/    OR @filterFlag/1%2=1  AND T.Hashtag = 0 AND T.Stock = 0 AND T.[User] = 0 AND T.NGram=0  
                     /*REM TermBigram*/     OR @filterFlag/2%2=1  AND T.Hashtag = 0 AND T.Stock = 0 AND T.[User] = 0 AND T.NGram=1  
                     /*REM UserUnigram*/    OR @filterFlag/4%2=1  AND T.Hashtag = 0 AND T.Stock = 0 AND T.[User] = 1 AND T.NGram=0  
                     /*REM HashtagUnigram*/ OR @filterFlag/8%2=1  AND T.Hashtag = 1 AND T.Stock = 0 AND T.[User] = 0 AND T.NGram=0  
                     /*REM HashtagBigram*/  OR @filterFlag/16%2=1 AND T.Hashtag = 1 AND T.Stock = 0 AND T.[User] = 0 AND T.NGram=1 
                     /*REM StockUnigram*/   OR @filterFlag/32%2=1 AND T.Hashtag = 0 AND T.Stock = 1 AND T.[User] = 0 AND T.NGram=0 
                     /*REM StockBigram*/    OR @filterFlag/64%2=1 AND T.Hashtag = 0 AND T.Stock = 1 AND T.[User] = 0 AND T.NGram=1 
                     )
           GROUP BY T.StemHash
           ORDER BY SUM(T.TFIDF) DESC           
           ) AS Terms