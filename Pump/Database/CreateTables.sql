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
	[StartTime] [datetime] NOT NULL,
	[RecordState] [int] NOT NULL
	constraint UQ_Terms unique (TableId, ClusterId, StemHash, RecordState)
) ON [PRIMARY]

GO

END