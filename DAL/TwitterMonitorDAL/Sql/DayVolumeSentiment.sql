/*REM*/ DECLARE @entity VARCHAR(MAX) = '9F1FE5B6-1025-676A-88E6-2796F14DDF8D'
/*REM*/ DECLARE @from DATE = '2013-05-01'
/*REM*/ DECLARE @to DATE = '2013-06-20'
/*REM*/ DECLARE @days INT = 12
/*REM*/ DECLARE @normalize BIT = 1
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @from DATE = {1}
--ADD DECLARE @to DATE = {2}
--ADD DECLARE @days INT = {3}
--ADD DECLARE @normalize BIT = {4}

DECLARE @sentStDevMod float = 6
DECLARE @sentStDev float = 1/@sentStDevMod

--Retrieveng normalization parameter - standard deviation of the first year of entity's data
IF (@normalize = 1) BEGIN
	DECLARE @fromNormalize DATE
	SELECT @fromNormalize = MIN(CAST(DATEADD(MILLISECOND,-10,D.EndTime) AS DATE))
	  FROM Tweets D
     WHERE     D.TableId = @entity
           AND D.RecordState = 0
    DECLARE @toNormalize DATE = DATEADD (DAY, 365, @fromNormalize)

	SELECT @sentStDev = COALESCE(STDEV(IndexByDays.[Index]), 1)
	  FROM (SELECT CAST(DATEADD(MILLISECOND,-10, D.EndTime) AS DATE) AS [Date],
                   SUM(D.Sentiment) / Count(1) / (@sentStDevMod*@sentStDev) AS [Index],
                   COUNT(1) AS Volume
              FROM Tweets D
             WHERE     D.TableId = @entity
                   AND D.RecordState = 0
                   AND CAST(DATEADD(MILLISECOND,-10,D.EndTime) AS DATE) >= @fromNormalize
                   AND CAST(DATEADD(MILLISECOND,-10,D.EndTime) AS DATE) <= @toNormalize
             GROUP BY CAST(DATEADD(MILLISECOND,-10, D.EndTime) AS DATE)
			) IndexByDays

/*REM*/ PRINT '@fromNormalize = ' + CONVERT(VARCHAR(MAX), @fromNormalize)
/*REM*/ PRINT '@fromNormalize + year = ' + CONVERT(VARCHAR(MAX), DATEADD (DAY, 365, @fromNormalize))
/*REM*/ PRINT '@sentStDev = ' + CONVERT(VARCHAR(MAX), @sentStDev)
END

--The actual select returned
SELECT CAST(DATEADD(MILLISECOND,-10, D.EndTime) AS DATE) AS [Date],
       SUM(D.Sentiment) / Count(1) / (@sentStDevMod*@sentStDev) AS [Index],
       COUNT(1) AS Volume
  FROM Tweets D
 WHERE     D.TableId = @entity
       AND D.RecordState = 0
       AND CAST(DATEADD(MILLISECOND,-10,D.EndTime) AS DATE) >= @from
       AND CAST(DATEADD(MILLISECOND,-10,D.EndTime) AS DATE) <= @to
 GROUP BY CAST(DATEADD(MILLISECOND,-10, D.EndTime) AS DATE)
 ORDER BY CAST(DATEADD(MILLISECOND,-10, D.EndTime) AS DATE)