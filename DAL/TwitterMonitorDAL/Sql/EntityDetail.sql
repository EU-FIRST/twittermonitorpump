/*REM*/ DECLARE @entity VARCHAR(MAX) = 'AAPL'
/*REM*/ DECLARE @windowSize VARCHAR(MAX) = 'D' 
--ADD DECLARE @entity VARCHAR(MAX) = {0}
--ADD DECLARE @windowSize VARCHAR(MAX) = {1} 
  
  SELECT @entity AS Entity,
         @windowSize AS WindowSize,
         Min(StartTime) StartTime,
		 Max(EndTime) EndTime,
         Count(1) AS NumOfDataPoints,
		 AVG(DateDiff(second, StartTime, EndTime)) AS TimeSpanResolutionSec
    FROM [AAPL_D_Clusters] Clusters
	where Clusters.RecordState = 0
