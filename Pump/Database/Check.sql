-- Check.sql

-- Checks if specific time period records already exist

SELECT * FROM Clusters WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 0