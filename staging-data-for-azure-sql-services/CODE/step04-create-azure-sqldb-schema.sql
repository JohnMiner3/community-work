/******************************************************
 *
 * Name:         step04-create-azure-sqldb-schema.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-10-2018
 *     Purpose:  Create the schema for the STOCK database.
 * 
 ******************************************************/

--
-- Create the database
--

-- Delete existing database
/*
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'db4stocks')
DROP DATABASE [db4stocks]
GO
*/

-- Create new database
/*
CREATE DATABASE [db4stocks]
(
MAXSIZE = 2GB,
EDITION = 'STANDARD',
SERVICE_OBJECTIVE = 'S0'
)
GO   
*/

--
-- Create ACTIVE schema
--

-- Delete existing schema.
DROP SCHEMA IF EXISTS [ACTIVE]
GO
 
-- Add new schema.
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO


--
-- Create STAGE schema
--

-- Delete existing schema.
DROP SCHEMA IF EXISTS [STAGE]
GO
 
-- Add new schema.
CREATE SCHEMA [STAGE] AUTHORIZATION [dbo]
GO


--
-- Create ACTIVE table
--
 
-- Delete existing table
DROP TABLE IF EXISTS [ACTIVE].[STOCKS]
GO
 
-- Create new table
CREATE TABLE [ACTIVE].[STOCKS]
(
  ST_ID INT IDENTITY(1, 1) NOT NULL,
  ST_SYMBOL VARCHAR(32) NOT NULL,
  ST_DATE DATE NOT NULL,
  ST_OPEN REAL NULL,
  ST_HIGH REAL NULL,
  ST_LOW REAL NULL,
  ST_CLOSE REAL NULL,
  ST_ADJ_CLOSE REAL NULL,
  ST_VOLUME BIGINT NULL,
  
  CONSTRAINT [PK_STOCKS_ID] PRIMARY KEY CLUSTERED (ST_ID ASC)
);



--
-- Create STAGE table
--
 
-- Delete existing table
DROP TABLE IF EXISTS [STAGE].[STOCKS]
GO
 
-- Create new table
CREATE TABLE [STAGE].[STOCKS]
(
  ST_SYMBOL VARCHAR(32) NOT NULL,
  ST_DATE DATE NOT NULL,
  ST_OPEN REAL NULL,
  ST_HIGH REAL NULL,
  ST_LOW REAL NULL,
  ST_CLOSE REAL NULL,
  ST_ADJ_CLOSE REAL NULL,
  ST_VOLUME BIGINT NULL
)


--
-- Create audit table
--
 
-- Delete existing table
DROP TABLE IF EXISTS [STAGE].[AUDIT]
GO

-- Create new table
CREATE TABLE [STAGE].[AUDIT]
(
	AU_CHANGE_ID INT IDENTITY (1,1) NOT NULL,
	AU_CHANGE_DATE [datetime] NOT NULL,
	AU_CMD_TEXT [varchar](1024) NOT NULL,
	AU_CHANGE_BY [nvarchar](256) NOT NULL,
	AU_APP_NAME [nvarchar](128) NOT NULL,
	AU_HOST_NAME [nvarchar](128) NOT NULL,

    CONSTRAINT [PK_CHANGE_ID] PRIMARY KEY CLUSTERED (AU_CHANGE_ID ASC)
) 
GO
 
-- Add defaults for key information
ALTER TABLE [STAGE].[AUDIT]
    ADD CONSTRAINT [DF_CHANGE_DATE] DEFAULT (GETDATE()) FOR AU_CHANGE_DATE;
 
ALTER TABLE [STAGE].[AUDIT]
    ADD CONSTRAINT [DF_CHANGE_TEXT] DEFAULT ('') FOR AU_CMD_TEXT;
 
ALTER TABLE [STAGE].[AUDIT]
    ADD CONSTRAINT [DF_CHANGE_BY] DEFAULT (COALESCE(SUSER_SNAME(),'?')) FOR AU_CHANGE_BY;
 
ALTER TABLE [STAGE].[AUDIT]
    ADD CONSTRAINT [DF_APP_NAME] DEFAULT (COALESCE(APP_NAME(),'?')) FOR AU_APP_NAME;
 
ALTER TABLE [STAGE].[AUDIT]
    ADD CONSTRAINT [DF_HOST_NAME] DEFAULT (COALESCE(CAST(CONNECTIONPROPERTY('client_net_address') AS nvarchar(128)),'?')) FOR AU_HOST_NAME;
GO
 
