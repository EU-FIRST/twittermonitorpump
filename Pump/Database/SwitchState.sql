-- SwitchState.sql

-- Changes record state

UPDATE Clusters SET RecordState = 2 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 0
UPDATE Clusters SET RecordState = 0 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 1
UPDATE Terms SET RecordState = 2 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 0
UPDATE Terms SET RecordState = 0 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 1
UPDATE Tweets SET RecordState = 2 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 0
UPDATE Tweets SET RecordState = 0 WHERE TableId = @TableId AND EndTime = @EndTime AND RecordState = 1