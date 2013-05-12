-- Initialize.sql

-- Removes records with a specific TableId

DELETE FROM Clusters WHERE TableId = @TableId 
DELETE FROM Terms WHERE TableId = @TableId 
DELETE FROM Tweets WHERE TableId = @TableId 