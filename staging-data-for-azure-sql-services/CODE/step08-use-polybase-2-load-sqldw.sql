/******************************************************
 *
 * Name:         step08-step08-use-polybase-2-load-sqldw.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-08-2017
 *     Purpose:  Use polybase to load azure sql data warehouse.
 * 
 ******************************************************/


--
-- Master Key
--

-- Drop master key
IF EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
DROP MASTER KEY;


-- Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Qkmof0SV3yxReKEP'; 
GO



--
-- Database Credential (storage key)
--

-- Drop db credential
IF EXISTS(SELECT * FROM sys.database_credentials WHERE name = 'CRD_AZURE_4_STOCKS')
DROP DATABASE SCOPED CREDENTIAL CRD_AZURE_4_STOCKS;  
GO 

-- Create db credential
CREATE DATABASE SCOPED CREDENTIAL CRD_AZURE_4_STOCKS 
WITH IDENTITY = 'STOCKS_USER', 
SECRET = 'V3nPC9wcXCR+WR6pRlUUKCk5bvX7MCyldSiGpjZeV5Q15vgQ9EkT1Bgi2WFiX10BbMDoSYjyxs8suvmNVEvo4A==';  
GO



--
-- External data src 
--

-- Drop external data src
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE NAME = 'EDS_AZURE_4_STOCKS')
DROP EXTERNAL DATA SOURCE [EDS_AZURE_4_STOCKS];
GO

-- Create external data src
CREATE EXTERNAL DATA SOURCE EDS_AZURE_4_STOCKS
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://sc4inbox@sa4adf18.blob.core.windows.net',
    CREDENTIAL = CRD_AZURE_4_STOCKS 
);
GO


--
-- External file format 
--

-- Drop external file format
IF EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'EFF_AZURE_4_STOCKS')
DROP EXTERNAL FILE FORMAT EFF_AZURE_4_STOCKS 
GO

-- Create external file format
CREATE EXTERNAL FILE FORMAT EFF_AZURE_4_STOCKS 
WITH 
(   FORMAT_TYPE = DELIMITEDTEXT,   
    FORMAT_OPTIONS  (   FIELD_TERMINATOR = ','
                    ,   STRING_DELIMITER = '"'
                    ,   DATE_FORMAT      = 'MM/dd/yyyy'
                    ,   USE_TYPE_DEFAULT = FALSE 
                    )
);
GO



--
-- Create a external (virtual) table
--

-- Drop external table
IF EXISTS (SELECT * FROM sys.external_tables WHERE NAME = 'PACKING_LST')
DROP EXTERNAL TABLE [BS].[PACKING_LST];
GO

-- Create external table
CREATE EXTERNAL TABLE [BS].[PACKING_LST]
(
    DATA_FILE_NAME [varchar] (128)
)
WITH
(
    LOCATION='NEW/PACKING-LIST.TXT' 
,   DATA_SOURCE = EDS_AZURE_4_STOCKS
,   FILE_FORMAT = EFF_AZURE_4_STOCKS
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 1
)
;



--
-- Show data
--

SELECT * 
FROM [BS].[PACKING_LST];
GO



--
-- Create a external (virtual) table
--

-- Drop external table
IF EXISTS (SELECT * FROM sys.external_tables WHERE NAME = 'MSFT')
DROP EXTERNAL TABLE [BS].[MSFT];
GO

-- Create external table
CREATE EXTERNAL TABLE [BS].[MSFT]
(
    yf_symbol [varchar] (64),
    yf_date [varchar] (64),
    yf_open [varchar] (64),
    yf_high [varchar] (64),
	yf_low [varchar] (64),
	yf_close [varchar] (64),
	yf_adjclose [varchar] (64),
	yf_volume [varchar] (64)
)
WITH
(
    LOCATION='NEW/MSFT-FY2013.CSV' 
,   DATA_SOURCE = EDS_AZURE_4_STOCKS
,   FILE_FORMAT = EFF_AZURE_4_STOCKS
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 1
)
;


--
-- Show data
--

SELECT * 
FROM [BS].[MSFT]
WHERE [yf_symbol] <> 'symbol'
ORDER BY [yf_date]
GO



--
-- Create view that supplies dynamic tsql
--

-- Delete existing view
IF OBJECT_ID('STAGE.TSQL_FOR_PACKING_LST_FILES', 'V') IS NOT NULL
DROP VIEW [STAGE].[TSQL_FOR_PACKING_LST_FILES]
GO


-- Create new view
CREATE VIEW [STAGE].[TSQL_FOR_PACKING_LST_FILES]
AS

-- Add dynamic T-SQL to temporary table
SELECT 

    -- Auto increment number
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS ROW_ID,

	-- Data file name
	DATA_FILE_NAME AS DATA_FILE,

	-- Drop external table if it exists
    'IF EXISTS (SELECT * FROM sys.external_tables WHERE NAME = ' + 
	CHAR(39) + SUBSTRING(DATA_FILE_NAME, 1, LEN(DATA_FILE_NAME) - 11) + CHAR(39) + ') ' + 
    'DROP EXTERNAL TABLE [BS].[' + SUBSTRING(DATA_FILE_NAME, 1, LEN(DATA_FILE_NAME) - 11) + '];' AS DROP_STMT,

	-- Create new external table
	'CREATE EXTERNAL TABLE [BS].[' + SUBSTRING(DATA_FILE_NAME, 1, LEN(DATA_FILE_NAME) - 11) + '] ' + 
    '( ' + 
	'yf_symbol [varchar] (64), ' +
    'yf_date [varchar] (64), ' +
    'yf_open [varchar] (64), ' +
    'yf_high [varchar] (64), ' +
	'yf_low [varchar] (64), ' +
	'yf_close [varchar] (64), ' +
	'yf_adjclose [varchar] (64), ' +
	'yf_volume [varchar] (64) ' +
    ') ' + 
	'WITH ' +
    '( ' +
    'LOCATION=' + CHAR(39) + 'NEW/' + DATA_FILE_NAME + CHAR(39) + ' ' +
    ', DATA_SOURCE = EDS_AZURE_4_STOCKS ' +
    ', FILE_FORMAT = EFF_AZURE_4_STOCKS ' +
    ', REJECT_TYPE = VALUE ' + 
    ', REJECT_VALUE = 1 ' +
    ') ' AS CREATE_STMT,

	-- Move data into staging table
	'INSERT INTO STAGE.STOCKS ' + 
    'SELECT ' +
    '  yf_symbol as my_symbol, ' +
    '  cast(yf_date as date) as my_date, ' +
	'  cast(yf_open as real) as my_open, ' +
	'  cast(yf_high as real) as my_high, ' +
	'  cast(yf_low as real) as my_low, ' +
	'  cast(yf_close as real) as my_close, ' +
	'  cast(yf_adjclose as real) as my_adjclose, ' +
	'  cast(yf_volume as bigint) as my_volume ' +
    'FROM [BS].[' + SUBSTRING(DATA_FILE_NAME, 1, LEN(DATA_FILE_NAME) - 11) + '] ' +
    'WHERE [yf_symbol] <> ' + CHAR(39) + 'symbol' + CHAR(39) + ';' AS INSERT_STMT

FROM 
    [BS].[PACKING_LST];
GO


-- Show the data
SELECT * 
FROM [STAGE].[TSQL_FOR_PACKING_LST_FILES] 
ORDER BY ROW_ID;
GO



--
-- Create s.p. to load from blob storage
--

-- Drop stored procedure
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[LOAD_FROM_BLOB_STORAGE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [ACTIVE].[LOAD_FROM_BLOB_STORAGE]
GO
 

-- Create stored procedure
CREATE PROCEDURE [ACTIVE].[LOAD_FROM_BLOB_STORAGE]
    @VAR_VERBOSE_FLAG CHAR(1) 
AS
BEGIN

  -- Local variables
  DECLARE @SQL_STMT NVARCHAR(4000) = '';
  DECLARE @DATA_FILE VARCHAR(128);
  DECLARE @CURR_DATE DATETIME = (SELECT GETDATE());
  DECLARE @USER_NAME VARCHAR(128) = (SELECT COALESCE(SUSER_SNAME(),'?'));

  -- Loop variables
  DECLARE @FILE_CNT INT = (SELECT COUNT(*) FROM [BS].[PACKING_LST]);
  DECLARE @LOOP_CNT INT = 1;

  -- Clear data from stage
  TRUNCATE TABLE [STAGE].[STOCKS];

  -- While loop replaces cursor
  WHILE @LOOP_CNT <= @FILE_CNT 
  BEGIN

    -- Debugging
    IF (@VAR_VERBOSE_FLAG = 'Y')
    BEGIN
        PRINT '[LOAD_FROM_BLOB_STORAGE] - START PROCESSING FILE ' + @DATA_FILE + '.'
        PRINT ' '
    END

    -- Current file name
	SET @SQL_STMT = (SELECT DATA_FILE FROM [STAGE].[TSQL_FOR_PACKING_LST_FILES] WHERE ROW_ID = @LOOP_CNT);


    -- Remove existing external table
	SET @SQL_STMT = (SELECT DROP_STMT FROM [STAGE].[TSQL_FOR_PACKING_LST_FILES] WHERE ROW_ID = @LOOP_CNT);
    EXEC sp_executesql @SQL_STMT;

	-- Insert Audit record
	INSERT INTO [STAGE].[AUDIT] ([AU_CHANGE_DATE],[AU_CMD_TEXT],[AU_CHANGE_BY],[AU_APP_NAME],[AU_HOST_NAME])
	VALUES ( @CURR_DATE, @SQL_STMT, @USER_NAME, 'Drop External Table', 'db4stocks' );


	-- Create existing external table
	SET @SQL_STMT = (SELECT CREATE_STMT FROM [STAGE].[TSQL_FOR_PACKING_LST_FILES] WHERE ROW_ID = @LOOP_CNT);
    EXEC sp_executesql @SQL_STMT;

	-- Insert Audit record
	INSERT INTO [STAGE].[AUDIT] ([AU_CHANGE_DATE],[AU_CMD_TEXT],[AU_CHANGE_BY],[AU_APP_NAME],[AU_HOST_NAME])
	VALUES ( @CURR_DATE, @SQL_STMT, @USER_NAME, 'Create External Table', 'db4stocks' );


	-- Insert into staging table
	SET @SQL_STMT = (SELECT INSERT_STMT FROM [STAGE].[TSQL_FOR_PACKING_LST_FILES] WHERE ROW_ID = @LOOP_CNT);
    EXEC sp_executesql @SQL_STMT;

	-- Insert Audit record
	INSERT INTO [STAGE].[AUDIT] ([AU_CHANGE_DATE],[AU_CMD_TEXT],[AU_CHANGE_BY],[AU_APP_NAME],[AU_HOST_NAME])
	VALUES ( @CURR_DATE, @SQL_STMT, @USER_NAME, 'Load Staging Table', 'db4stocks' );


    -- Debugging
    IF (@VAR_VERBOSE_FLAG = 'Y')
    BEGIN
        PRINT '[LOAD_FROM_BLOB_STORAGE] - END PROCESSING FILE ' + @DATA_FILE + '.'
        PRINT ' '
    END

	-- Increment the counter
    SET @LOOP_CNT +=1;

  END

END
GO



--
-- Test load process
--

-- Import blob files into stage
EXEC [ACTIVE].[LOAD_FROM_BLOB_STORAGE] @VAR_VERBOSE_FLAG = 'Y';
GO


-- Move from stage to active
INSERT INTO [ACTIVE].[STOCKS]
SELECT * 
FROM [STAGE].[STOCKS]
WHERE ST_OPEN <> 0 AND ST_CLOSE <> 0
GO


-- Validate data
SELECT 
    [ST_SYMBOL], 
	COUNT(*) AS TRADING_DAYS
FROM 
    [ACTIVE].[STOCKS]
GROUP BY 
    [ST_SYMBOL]
ORDER BY 
    TRADING_DAYS;
GO
