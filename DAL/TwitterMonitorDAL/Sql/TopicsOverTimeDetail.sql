/*REM*/ DECLARE @filterFlag INT = 1
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-02-01T01:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-04-01T00:00:00' 
/*REM*/ DECLARE @stepTimeSpan INT = 2
--ADD DECLARE @filterFlag INT = {0}
--ADD DECLARE @dateTimeStart DATETIME = {1} 
--ADD DECLARE @dateTimeEnd DATETIME = {2}
--ADD DECLARE @stepTimeSpan INT = {3}

  SELECT TopicId,
         TopicNumDocs,
         TimeSlotNumDocs,
         TimeSlotGroup,
         StartTime,
         EndTime,
         Term,
         Weight
    FROM (
            SELECT Topic TopicId,
                   Topic.NumDocs TopicNumDocs,
                   Sum(Clusters.NumDocs) TimeSlotNumDocs,
                   DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan TimeSlotGroup,
                   Min(StartTime) StartTime,
                   Max(EndTime) EndTime
              FROM [AAPL_D_Clusters] Clusters
                   INNER JOIN
                   (     SELECT /*REM*/ TOP 10
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
          GROUP BY Topic,
                   Topic.NumDocs,
                   DATEDIFF(hour, @dateTimeStart, StartTime)/@stepTimeSpan
         ) TopicsOverTime
         CROSS APPLY 
		 (
            SELECT /*REM*/ TOP 5
                   --ADD   TOP /*#NumTermsPerTimeSlot*/
                   Min(MostFrequentForm) Term,
                   Sum(TFIDF) Weight
              FROM [AAPL_D_Clusters] Clusters
                   INNER JOIN [AAPL_D_Terms] Terms
                       ON (Clusters.Id = Terms.ClusterId)
             WHERE Clusters.StartTime >= @dateTimeStart AND
                   Clusters.EndTime <= @dateTimeEnd AND
                   Clusters.Topic = TopicsOverTime.TopicId AND
                   ( 0=1
                     /*REM TermUnigram*/    OR @filterFlag/1%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=0  
                     /*REM TermBigram*/     OR @filterFlag/2%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=1  
                     /*REM UserUnigram*/    OR @filterFlag/4%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 1 AND Terms.NGram=0  
                     /*REM HashtagUnigram*/ OR @filterFlag/8%2=1  AND Terms.Hashtag = 1 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=0  
                     /*REM HashtagBigram*/  OR @filterFlag/16%2=1 AND Terms.Hashtag = 1 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=1 
                     /*REM StockUnigram*/   OR @filterFlag/32%2=1 AND Terms.Hashtag = 0 AND Terms.Stock = 1 AND Terms.[User] = 0 AND Terms.NGram=0 
                     /*REM StockBigram*/    OR @filterFlag/64%2=1 AND Terms.Hashtag = 0 AND Terms.Stock = 1 AND Terms.[User] = 0 AND Terms.NGram=1 
                   )
          GROUP BY StemHash,
		           DATEDIFF(hour, @dateTimeStart, Clusters.StartTime)/@stepTimeSpan
		    HAVING DATEDIFF(hour, @dateTimeStart, Clusters.StartTime)/@stepTimeSpan = TopicsOverTime.TimeSlotGroup
          ORDER BY Sum(TFIDF) DESC
         ) AS Terms
ORDER BY TopicsOverTime.TopicId,
         TopicsOverTime.TimeSlotGroup,
		 Terms.Weight DESC