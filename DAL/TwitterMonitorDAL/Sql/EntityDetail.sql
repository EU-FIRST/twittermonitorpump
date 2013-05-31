/*REM*/ DECLARE @entity VARCHAR(MAX) = 'MSFT'
/*REM*/ DECLARE @windowSize VARCHAR(MAX) = 'W' 
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1}

SELECT E.Name                   AS Entity,
       E.WindowCode             AS WindowSize,
       MIN(StartTime)           AS StartTime,
       MAX(EndTime)             AS EndTime,
       COUNT(1)                 AS NumOfDataPoints,
       E.ResolutionMinutes * 60 AS TimeSpanResolutionSec
  FROM Entity E
       INNER JOIN Clusters C
               ON C.TableId = E.TableId
 WHERE E.Name = @entity
       AND E.WindowCode = @windowSize
       AND C.RecordState = 0
 GROUP BY E.Name,
          E.WindowCode,
          E.ResolutionMinutes