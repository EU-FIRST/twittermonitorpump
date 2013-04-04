SELECT 
	SUBSTRING(Clusters.name, 10, LEN(Clusters.name) - 16) AS Entity, 
	SUBSTRING(Clusters.name, LEN(Clusters.name) - 5, 1) AS WindowSize
FROM sys.Tables Clusters INNER JOIN sys.Tables Terms
	ON SUBSTRING(Clusters.name, 10, 999) = SUBSTRING(Terms.name, 7, 999)