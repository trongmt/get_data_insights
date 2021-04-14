CREATE DATABASE [snp_fanpage];

USE [snp_fanpage]
GO

/****** Object:  Table [dbo].[PageFans]    Script Date: 4/14/2021 10:29:06 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PageFans](
	[ID] [varchar](100) NULL,
	[Period] [varchar](10) NULL,
	[Name] [varchar](100) NULL,
	[EndTime] [date] NULL,
	[Title] [nvarchar](max) NULL,
	[Description] [nvarchar](max) NULL,
	[Attribute] [varchar](2) NULL,
	[Value] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

USE [snp_fanpage]
GO

/****** Object:  Table [dbo].[PageInsight]    Script Date: 4/14/2021 10:29:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PageInsight](
	[ID] [varchar](100) NULL,
	[Period] [varchar](10) NULL,
	[Name] [varchar](100) NULL,
	[Value] [int] NULL,
	[EndTime] [date] NULL,
	[Title] [nvarchar](max) NULL,
	[Description] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

alter table [PostInsight] add PostID varchar(100);

/****** Object:  Table [dbo].[Post]    Script Date: 4/14/2021 10:29:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Post](
	[ID] [varchar](100) NULL,
	[Message] [nvarchar](max) NULL,
	[PostDate] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[PostActivity]    Script Date: 4/14/2021 10:30:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PostActivity](
	[ID] [varchar](100) NULL,
	[Period] [varchar](10) NULL,
	[Name] [varchar](100) NULL,
	[Title] [nvarchar](max) NULL,
	[Description] [nvarchar](max) NULL,
	[Share] [int] NULL,
	[Like] [int] NULL,
	[Comment] [int] NULL,
	[PostID] [varchar](100) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[PostClick]    Script Date: 4/14/2021 10:31:05 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PostClick](
	[ID] [varchar](100) NULL,
	[Period] [varchar](10) NULL,
	[Name] [varchar](100) NULL,
	[Title] [nvarchar](max) NULL,
	[Description] [nvarchar](max) NULL,
	[OtherClicks] [int] NULL,
	[LinkClicks] [int] NULL,
	[PhotoView] [int] NULL,
	[VideoPlay] [int] NULL,
	[PostID] [varchar](100) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[PostInsight]    Script Date: 4/14/2021 10:31:25 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PostInsight](
	[ID] [varchar](100) NULL,
	[Period] [varchar](10) NULL,
	[Name] [varchar](100) NULL,
	[Title] [nvarchar](max) NULL,
	[Value] [int] NULL,
	[Description] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO