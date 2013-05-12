-- Read.sql

-- Reads tweets

SELECT t.Id, t.Text, t.CreatedAt FROM (
	SELECT DISTINCT ttq.TweetId FROM Queries q
	JOIN TweetToQuery ttq ON ttq.QueryId = q.Id
	WHERE q.IdStr = @IdStr AND ttq.TweetId > @Id) foo
JOIN Tweets t on t.Id = foo.TweetId
-- WHERE t.CreatedAt >= '2011-12-01' AND t.CreatedAt < '2013-01-01' 
ORDER BY t.Id