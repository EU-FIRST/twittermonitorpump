  SELECT SUBSTRING(Clusters.name, 0, LEN(Clusters.name) - 10) AS Entity,
         SUBSTRING(Clusters.name, LEN(Clusters.name)-9, 1) AS WindowSize
    FROM sys.Tables Clusters
	     INNER JOIN sys.Tables Terms
		     ON SUBSTRING(Clusters.name, 0, LEN(Clusters.name) - 7) = SUBSTRING(Terms.name, 0, LEN(Terms.name) - 4)
   