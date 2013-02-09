/*REM*/ DECLARE @filterFlag INT = 0
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T00:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-03-30T00:00:00' 
--ADD DECLARE @filterFlag INT = {0}
--ADD DECLARE @dateTimeStart DATETIME = {1} 
--ADD DECLARE @dateTimeEnd DATETIME = {2}

  SELECT /*REM*/ TOP 100
         --ADD   TOP /*#NumTerms*/
         Min(MostFrequentForm) Term,
         Sum(TFIDF) Weight
    FROM [AAPL_D_Clusters] Clusters
	     INNER JOIN [AAPL_D_Terms] Terms
		     ON (Clusters.Id = Terms.ClusterId)
   WHERE Clusters.StartTime >= @dateTimeStart AND
         Clusters.EndTime <= @dateTimeEnd AND
         ( 0=1
           /*REM TermUnigram*/    OR @filterFlag/1%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=0  
           /*REM TermBigram*/     OR @filterFlag/2%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=1  
           /*REM UserUnigram*/    OR @filterFlag/4%2=1  AND Terms.Hashtag = 0 AND Terms.Stock = 0 AND Terms.[User] = 1 AND Terms.NGram=0  
           /*REM HashtagUnigram*/ OR @filterFlag/8%2=1  AND Terms.Hashtag = 1 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=0  
           /*REM HashtagBigram*/  OR @filterFlag/16%2=1 AND Terms.Hashtag = 1 AND Terms.Stock = 0 AND Terms.[User] = 0 AND Terms.NGram=1 
           /*REM StockUnigram*/   OR @filterFlag/32%2=1 AND Terms.Hashtag = 0 AND Terms.Stock = 1 AND Terms.[User] = 0 AND Terms.NGram=0 
           /*REM StockBigram*/    OR @filterFlag/64%2=1 AND Terms.Hashtag = 0 AND Terms.Stock = 1 AND Terms.[User] = 0 AND Terms.NGram=1 
         )

GROUP BY StemHash
ORDER BY Sum(TFIDF) DESC