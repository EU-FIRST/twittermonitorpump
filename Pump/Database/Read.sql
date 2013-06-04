-- Read.sql

-- Reads tweets

SELECT t.Id, t.Text, t.CreatedAt FROM (
	SELECT DISTINCT ttq.TweetId FROM Queries q WITH (NOLOCK)
	JOIN TweetToQuery ttq WITH (NOLOCK)
	ON ttq.QueryId = q.Id
	WHERE q.IdStr = @IdStr AND ttq.TweetId > @Id) foo 
JOIN Tweets t WITH (NOLOCK)
ON t.Id = foo.TweetId
WHERE t.CreatedAt >= @MinTweetTimestamp
ORDER BY t.Id