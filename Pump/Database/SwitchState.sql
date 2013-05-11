-- SwitchState.sql

-- Changes record state

UPDATE Clusters SET RecordState = 2 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0
UPDATE Clusters SET RecordState = 0 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 1
UPDATE Terms SET RecordState = 2 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 0
UPDATE Terms SET RecordState = 0 WHERE TableId = @TableId AND StartTime = @StartTime AND RecordState = 1