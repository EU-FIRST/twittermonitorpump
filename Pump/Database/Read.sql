﻿-- Read.sql

-- Reads tweets

SELECT t.Id, t.Text, t.CreatedAt FROM (
	SELECT DISTINCT ttq.TweetId FROM Queries q
	JOIN TweetToQuery ttq ON ttq.QueryId = q.Id
	WHERE q.IdStr = @IdStr AND ttq.TweetId > @Id) foo
JOIN Tweets t on t.Id = foo.TweetId
WHERE t.CreatedAt >= @MinTweetTime
ORDER BY t.Id