/*REM*/ DECLARE @filterFlag INT = 1
/*REM*/ DECLARE @dateTimeStart DATETIME = '2012-01-01T00:00:00'
/*REM*/ DECLARE @dateTimeEnd DATETIME = '2012-02-01T00:00:00' 
--ADD DECLARE @filterFlag INT = {0}
--ADD DECLARE @dateTimeStart DATETIME = {1} 
--ADD DECLARE @dateTimeEnd DATETIME = {2}

  SELECT 
         /*REM*/ TOP 100
		 --TOP NN
         Min(MostFrequentForm) Term,
         Sum(TFIDF) Weight
    FROM [AAPL_D_Clusters] Clusters
	     INNER JOIN [AAPL_D_Terms] Terms
		     ON (Clusters.Id = Terms.ClusterId)
   WHERE Clusters.StartTime >= @dateTimeStart AND
         Clusters.EndTime <= @dateTimeEnd
GROUP BY StemHash
ORDER BY Sum(TFIDF) DESC