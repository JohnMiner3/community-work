/******************************************************
 *
 * Name:         hybrid-data-platform-solutions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-07-2016
 *     Purpose:  A sample banking database.
 * 
 ******************************************************/



/*  
	1 - Create a database for banking information
*/


-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING02')
 DROP DATABASE [BANKING02]
GO


-- Add new databases.
CREATE DATABASE [BANKING02] ON  
 PRIMARY 
  ( NAME = N'BANKING_PRI_DAT', FILENAME = N'C:\MSSQL\DATA\BANKING02.MDF', SIZE = 16MB, FILEGROWTH = 16MB),
 FILEGROUP DATA2014
  ( NAME =  'BANKING_2014_DAT', FILENAME = N'C:\MSSQL\DATA\BANKING02-2014.NDF', SIZE = 16MB, FILEGROWTH = 16MB),
 FILEGROUP DATA2015
  ( NAME =  'BANKING_2015_DAT', FILENAME = N'C:\MSSQL\DATA\BANKING02-2015.NDF', SIZE = 16MB, FILEGROWTH = 16MB)
 LOG ON 
  ( NAME = N'BANKING_ALL_LOG', FILENAME = N'C:\MSSQL\LOG\BANKING02.LDF' , SIZE = 16MB, FILEGROWTH = 16MB)
GO


/*
   2 - Create Active schema
*/

-- Which database to use.
USE [BANKING02]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ACTIVE')
DROP SCHEMA [ACTIVE]
GO

-- Add new schema.
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'ACTIVE';
GO



/*
   3 - Create history schema
*/

-- Which database to use.
USE [BANKING02]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'HISTORY')
DROP SCHEMA [HISTORY]
GO

-- Add new schema.
CREATE SCHEMA [HISTORY] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'HISTORY';
GO



/*
   4 - Move over account data
*/

-- Add to primary file group
SELECT * 
INTO [BANKING02].[ACTIVE].[ACCOUNTS]
FROM [BANKING01].[ACTIVE].[ACCOUNTS];
GO


/*
   4 - Move over customer data
*/

-- Add to primary file group
SELECT * 
INTO [BANKING02].[ACTIVE].[CUSTOMER]
FROM [BANKING01].[ACTIVE].[CUSTOMER];
GO


/*
   5 - Move over 2014 data as history
*/


ALTER DATABASE [BANKING02]
MODIFY FILEGROUP DATA2014 DEFAULT;
GO

-- Add to first secondary fg
SELECT * 
INTO [BANKING02].[HISTORY].[TRANSACTION]
FROM [BANKING01].[ACTIVE].[TRANSACTION] T
WHERE YEAR(T.TRAN_DATE) = 2014;
GO



/*
   6 - Move over 2015 data as active
*/


ALTER DATABASE [BANKING02]
MODIFY FILEGROUP DATA2015 DEFAULT;
GO

-- Add to second secondary fg
SELECT * 
INTO [BANKING02].[ACTIVE].[TRANSACTION]
FROM [BANKING01].[ACTIVE].[TRANSACTION] T
WHERE YEAR(T.TRAN_DATE) = 2015;
GO



/*
   7 - Create all schema
*/

-- Which database to use.
USE [BANKING02]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ALL')
DROP SCHEMA [ALL]
GO

-- Add new schema.
CREATE SCHEMA [ALL] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'ALL';
GO



/*
   8 - Add date dim and primary key
*/

-- Computed column
ALTER TABLE [ACTIVE].[TRANSACTION] ADD DATE_KEY AS ISNULL((YEAR(TRAN_DATE) * 100 + MONTH(TRAN_DATE)), '190001') PERSISTED;
GO

-- Primary key
ALTER TABLE [ACTIVE].[TRANSACTION] 
ADD CONSTRAINT [PK_ACTIVE_TRANSACTION] PRIMARY KEY CLUSTERED ([DATE_KEY], [TRAN_ID])
WITH (FILLFACTOR = 85, PAD_INDEX = ON) ON [DATA2015];
GO


-- Computed column
ALTER TABLE [HISTORY].[TRANSACTION] ADD DATE_KEY AS ISNULL((YEAR(TRAN_DATE) * 100 + MONTH(TRAN_DATE)), '190001') PERSISTED;
GO

-- Primary key
ALTER TABLE [HISTORY].[TRANSACTION] 
ADD CONSTRAINT [PK_HISTORY_TRANSACTION] PRIMARY KEY CLUSTERED ([DATE_KEY], [TRAN_ID])
WITH (FILLFACTOR = 85, PAD_INDEX = ON) ON [DATA2014];
GO



/*
   9 - Use a partition view
*/


-- Default to primary
ALTER DATABASE [BANKING02]
MODIFY FILEGROUP [PRIMARY] DEFAULT;
GO

-- Create view for all transactions
CREATE VIEW [ALL].[TRANSACTIONS] WITH SCHEMABINDING AS

SELECT
    [TRAN_ID]
   ,[TRAN_AMOUNT]
   ,[TRAN_DATE]
   ,[ACCT_ID]
   ,[DATE_KEY]
FROM 
    [ACTIVE].[TRANSACTION]

UNION ALL

SELECT
    [TRAN_ID]
   ,[TRAN_AMOUNT]
   ,[TRAN_DATE]
   ,[ACCT_ID]
   ,[DATE_KEY]
FROM 
    [HISTORY].[TRANSACTION]
GO


/*
   10 - Show the query optimizer picking right table
*/


-- Traverses the history pk
SELECT * 
FROM [ALL].[TRANSACTIONS] 
WHERE [DATE_KEY] < 201501;
GO