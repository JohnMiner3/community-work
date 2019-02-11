/******************************************************
 *
 * Name:         step07-create-azure-sqldw-schema.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-08-2017
 *     Purpose:  Create the schemas & tables.
 * 
 ******************************************************/


--
-- Create a blob storage schema
--

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'BS')
DROP SCHEMA [BS]
GO
 
-- Add new schema.
CREATE SCHEMA [BS] AUTHORIZATION [dbo]
GO


--
-- Create a stage schema
--

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'STAGE')
DROP SCHEMA [STAGE]
GO
 
-- Add new schema.
CREATE SCHEMA [STAGE] AUTHORIZATION [dbo]
GO


--
-- Create active schema
--

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'ACTIVE')
DROP SCHEMA [ACTIVE]
GO
 
-- Add new schema.
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO



--
-- Create active stocks table
--
 
-- Delete existing table
IF OBJECT_ID('ACTIVE.STOCKS', 'U') IS NOT NULL
DROP TABLE [ACTIVE].[STOCKS]
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
  ST_VOLUME BIGINT NULL
)
WITH ( DISTRIBUTION = ROUND_ROBIN );
GO


--
-- Create stage stocks table
--
 
-- Delete existing table
IF OBJECT_ID('STAGE.STOCKS', 'U') IS NOT NULL
DROP TABLE [STAGE].[STOCKS]
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
WITH ( DISTRIBUTION = ROUND_ROBIN );
GO


--
-- Create audit load table
--
 
-- Delete existing table
IF OBJECT_ID('STAGE.AUDIT', 'U') IS NOT NULL
DROP TABLE [STAGE].[AUDIT]
GO
 
-- Create new table
CREATE TABLE [STAGE].[AUDIT]
(
	AU_CHANGE_ID INT IDENTITY (1,1) NOT NULL,
	AU_CHANGE_DATE [datetime] NOT NULL,
	AU_CMD_TEXT [varchar](1024) NOT NULL,
	AU_CHANGE_BY [nvarchar](256) NOT NULL,
	AU_APP_NAME [nvarchar](128) NOT NULL,
	AU_HOST_NAME [nvarchar](128) NOT NULL
) 
WITH ( DISTRIBUTION = ROUND_ROBIN );
GO


--
-- Does not support Primary Keys & Constraints with dynamic values
--

/*

https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse

-- Add defaults for key information
SELECT GETDATE()
SELECT (COALESCE(SUSER_SNAME(),'?')) 
SELECT (COALESCE(APP_NAME(),'?')) 
SELECT (COALESCE(CAST(CONNECTIONPROPERTY('client_net_address') AS nvarchar(128)),'?'))

*/




