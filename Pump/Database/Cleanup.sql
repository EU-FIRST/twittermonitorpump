-- Cleanup.sql

-- Removes records with state other than 0

DELETE FROM Clusters WHERE TableId = @TableId AND RecordState <> 0
DELETE FROM Terms WHERE TableId = @TableId AND RecordState <> 0
DELETE FROM Tweets WHERE TableId = @TableId AND RecordState <> 0