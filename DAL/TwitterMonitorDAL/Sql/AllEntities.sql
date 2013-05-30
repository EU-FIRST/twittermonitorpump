SELECT E.Name       AS Entity,
       E.WindowCode AS WindowSize
  FROM Entity E
       INNER JOIN Clusters C
               ON C.TableId = E.TableId
 WHERE C.RecordState = 0               
 GROUP BY E.Name,
          E.WindowCode