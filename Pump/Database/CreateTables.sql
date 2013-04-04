IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Clusters]') AND type in (N'U'))
DROP TABLE [dbo].[Clusters]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Terms]') AND type in (N'U'))
DROP TABLE [dbo].[Terms]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Clusters](
	[Id] [uniqueidentifier] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[Topic] [bigint] NOT NULL,
	[NumDocs] [int] NOT NULL,
	[RecordState] [int] NOT NULL
	constraint UQ_Clusters unique (Id, RecordState) 
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[Terms](
	[ClusterId] [uniqueidentifier] NOT NULL,
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
	[EndTime] [datetime] NOT NULL,
	[RecordState] [int] NOT NULL
	constraint UQ_Terms unique (ClusterId, StemHash, RecordState)
) ON [PRIMARY]

GO