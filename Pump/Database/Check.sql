-- Check.sql

-- Checks if a specified time period records already exist

SELECT TOP 1 * FROM Clusters WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0