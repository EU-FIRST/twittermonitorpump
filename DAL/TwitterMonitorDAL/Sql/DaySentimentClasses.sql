/*REM*/ DECLARE @entity VARCHAR(MAX) = '9F1FE5B6-1025-676A-88E6-2796F14DDF8D'
/*REM*/ DECLARE @from DATE = '2013-05-01'
/*REM*/ DECLARE @to DATE = '2013-07-02'
--ADD DECLARE @entity VARCHAR(max) = {0}
--ADD DECLARE @from DATE = {1}
--ADD DECLARE @to DATE = {2}

SELECT CAST(DATEADD(MILLISECOND,-10, C.EndTime) AS DATE) AS [Date],
       SUM(C.SentimentPos) AS Positives, 
       SUM(C.SentimentPosLowCfd) AS PosNeutrals,
       SUM(C.SentimentNegLowCfd) AS NegNeutrals,
       SUM(C.SentimentNeg) AS Negatives,
       SUM(C.SentimentPos + C.SentimentPosLowCfd + C.SentimentNegLowCfd + C.SentimentNeg) AS Volume
  FROM Clusters C
 WHERE     C.TableId = @entity
       AND C.RecordState = 0
       AND CAST(DATEADD(MILLISECOND,-10,C.EndTime) AS DATE) >= @from
       AND CAST(DATEADD(MILLISECOND,-10,C.EndTime) AS DATE) <= @to
 GROUP BY CAST(DATEADD(MILLISECOND,-10, C.EndTime) AS DATE)
 ORDER BY CAST(DATEADD(MILLISECOND,-10, C.EndTime) AS DATE)