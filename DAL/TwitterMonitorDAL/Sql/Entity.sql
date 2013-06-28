/*REM*/ DECLARE @uri VARCHAR(max) = '9F1FE5B6-1025-676A-88E6-2796F14DDF8D'
/*REM*/ DECLARE @labelLike VARCHAR(max) = ''
--ADD DECLARE @uri VARCHAR(MAX) = {0}
--ADD DECLARE @labelLike VARCHAR(MAX) = {1}

SELECT CONVERT(VARCHAR(max), E.TableId) AS EntityUri,
	   E.Name    AS EntityLabel,
       '{ resolutionMinutes:' + CONVERT(VARCHAR, E.ResolutionMinutes)  + ', ' +
       'granularity:"' + E.Granularity + '", ' +
       'windowCode:"' + E.WindowCode + '", ' +
       'windowDays:' +  CONVERT(VARCHAR, E.WindowDays) +
       ' }'AS Features
  FROM Entity E
       INNER JOIN Clusters C
               ON C.TableId = E.TableId
 WHERE                   1=0
       /*REM uri*/       OR E.TableId = @uri 
       /*REM labelLike*/ OR E.Name LIKE '%' + @labelLike + '%'
 GROUP BY E.TableId,
          E.Name,
          E.ResolutionMinutes,
          E.Granularity,
          E.WindowCode,
          E.WindowDays