IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[BagsOfWords]') AND type in (N'U'))
DROP TABLE [dbo].[BagsOfWords]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BagsOfWords](
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[Topic1] [nvarchar](100) NULL,
	[Topic2] [nvarchar](100) NULL,
	[Topic3] [nvarchar](100) NULL,
	[WindowSize] [int] NOT NULL,
	[NumDocs] [int] NOT NULL,
	[Stem] [nvarchar](140) NOT NULL,
	[MostFrequentForm] [nvarchar](140) NOT NULL,
	[TF] [int] NOT NULL,
	[D] [int] NOT NULL,
	[TFIDF] [float] NOT NULL,
	[User] [bit] NOT NULL,
	[Hashtag] [bit] NOT NULL,
	[Stock] [bit] NOT NULL,
	[NGram] [bit] NOT NULL,
) ON [PRIMARY]

GO