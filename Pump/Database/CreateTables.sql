-- CreateTables.sql

-- Creates tables if they don't exist

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Clusters]') AND type in (N'U'))
BEGIN 

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Clusters](
	[TableId] [uniqueidentifier] NOT NULL,
	[Id] [uniqueidentifier] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[Topic] [bigint] NOT NULL,
	[NumDocs] [int] NOT NULL,	
	[SentimentBasicPos] [int] NOT NULL,
	[SentimentBasicNeg] [int] NOT NULL,
	[SentimentBasicNeutral] [int] NOT NULL,
	[SentimentBasicPosLowCfd] [int] NOT NULL,
	[SentimentBasicNegLowCfd] [int] NOT NULL,
	[SentimentPos] [int] NOT NULL,
	[SentimentNeg] [int] NOT NULL,
	[SentimentNeutral] [int] NOT NULL,
	[SentimentPosLowCfd] [int] NOT NULL,
	[SentimentNegLowCfd] [int] NOT NULL,
	[SentimentHandLabeledPos] [int] NOT NULL,
	[SentimentHandLabeledNeg] [int] NOT NULL,
	[SentimentHandLabeledNeutral] [int] NOT NULL,
	[RecordState] [int] NOT NULL
	constraint UQ_Clusters unique (TableId, Id, RecordState) 
) ON [PRIMARY]

GO

END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Terms]') AND type in (N'U'))
BEGIN

CREATE TABLE [dbo].[Terms](
	[TableId] [uniqueidentifier] NOT NULL,
	[ClusterId] [uniqueidentifier] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[StemHash] [uniqueidentifier] NOT NULL,
	[Stem] [nvarchar](140) NOT NULL,	
	[MostFrequentForm] [nvarchar](140) NOT NULL,
	[TF] [int] NOT NULL,
	[D] [int] NOT NULL,
	[TFIDF] [float] NOT NULL,
	[User] [bit] NOT NULL,
	[Hashtag] [bit] NOT NULL,
	[Stock] [bit] NOT NULL,
	[NGram] [bit] NOT NULL,
	[Tagged] [bit] NOT NULL,	
	[RecordState] [int] NOT NULL
	constraint UQ_Terms unique (TableId, ClusterId, StemHash, RecordState)
) ON [PRIMARY]

GO

END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Tweets]') AND type in (N'U'))
BEGIN

CREATE TABLE [dbo].[Tweets](
	[TableId] [uniqueidentifier] NOT NULL,
	[ClusterId] [uniqueidentifier] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[TweetId] [bigint] NOT NULL,
	[SentimentBasic] [float] NOT NULL,
	[SentimentBasicPos] [bit] NOT NULL,
	[SentimentBasicNeg] [bit] NOT NULL,
	[SentimentBasicNeutral] [bit] NOT NULL,
	[SentimentBasicPosLowCfd] [bit] NOT NULL,
	[SentimentBasicNegLowCfd] [bit] NOT NULL,
	[Sentiment] [float] NOT NULL,
	[SentimentPos] [bit] NOT NULL,
	[SentimentNeg] [bit] NOT NULL,
	[SentimentNeutral] [bit] NOT NULL,
	[SentimentPosLowCfd] [bit] NOT NULL,
	[SentimentNegLowCfd] [bit] NOT NULL,
	[Basic] [bit] NOT NULL,
	[HandLabeled] [bit] NOT NULL,
	[RecordState] [int] NOT NULL
	constraint UQ_Tweets unique (TableId, ClusterId, TweetId, RecordState)
) ON [PRIMARY]

END