/*REM*/ DECLARE @entity VARCHAR(MAX) = '9F1FE5B6-1025-676A-88E6-2796F14DDF8D'
--ADD DECLARE @entity VARCHAR(MAX) = {0}

SELECT 0                                AS Id,
       CONVERT(VARCHAR(max), E.TableId) AS EntityUri,
	   E.Name                           AS EntityLabel,
	   E.WindowCode                     AS Flags,
	   0                                AS ClassId,
       Count(1)                         AS NumOccurrences,
       Count(DISTINCT D.TweetId)        AS NumDocuments,
       Min(C.StartTime)                 AS DataStartTime,
       Max(C.EndTime)                   AS DataEndTime,
       '{ resolutionMinutes:' + CONVERT(VARCHAR, E.ResolutionMinutes)  + ', ' +
       'granularity:"' + E.Granularity + '", ' +
       'windowCode:"' + E.WindowCode + '", ' +
       'windowDays:' +  CONVERT(VARCHAR, E.WindowDays) +
       ' }'AS Features
  FROM Entity E
       INNER JOIN Clusters C
              ON C.TableId = E.TableId
       INNER JOIN Tweets D
              ON C.TableId = D.TableId AND
                 C.Id = D.ClusterId
 WHERE     E.TableId = @entity
       AND C.RecordState = 0
 GROUP BY E.TableId,
          E.Name,       
          E.WindowCode,
          E.Granularity,
          E.ResolutionMinutes,
          E.WindowDays