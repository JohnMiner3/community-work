/******************************************************
 *
 * Name:         step05-bulk-insert-from-blob-storage.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-10-2018
 *     Purpose:  Load the Azure SQL database
 *               using BULK INSERT and OPENROWSET.
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
-- Database Credential 
--

-- Drop db credential
IF EXISTS(SELECT * FROM sys.database_credentials WHERE name = 'CRD_AZURE_4_STOCKS')
DROP DATABASE SCOPED CREDENTIAL CRD_AZURE_4_STOCKS;  
GO 

-- Create db credential
CREATE DATABASE SCOPED CREDENTIAL CRD_AZURE_4_STOCKS
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2017-07-29&sr=c&si=pol4inbox&sig=MJBICz66qeCuw%2Bq%2FQaBkOw1tSmrKuEgpFIECW3EjoMg%3D';
GO



--
-- External data src 
--

-- Drop external data src
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE NAME = 'EDS_AZURE_4_STOCKS')
DROP EXTERNAL DATA SOURCE EDS_AZURE_4_STOCKS;
GO

-- Create external data src
CREATE EXTERNAL DATA SOURCE EDS_AZURE_4_STOCKS
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://sa4stg18.blob.core.windows.net/sc4inbox',
    CREDENTIAL = CRD_AZURE_4_STOCKS
);
GO



--
-- One big string
--

SELECT *
FROM 
OPENROWSET
(
    BULK 'NEW/PACKING-LIST.TXT',
    DATA_SOURCE = 'EDS_AZURE_4_STOCKS', 
    SINGLE_CLOB
) AS RAW_DATA


--
-- Must be using 2016 compatibility level
--

ALTER DATABASE [STOCKS] SET COMPATIBILITY_LEVEL = 130 
GO


--
-- Packing List
--

SELECT 
    CAST(LIST_DATA.VALUE AS VARCHAR(256)) AS PKG_LIST
FROM 
    OPENROWSET
    (
        BULK 'NEW/PACKING-LIST.TXT',
        DATA_SOURCE = 'EDS_AZURE_4_STOCKS', 
        SINGLE_CLOB
    ) AS RAW_DATA
CROSS APPLY 
    STRING_SPLIT(REPLACE(REPLACE(RAW_DATA.BulkColumn, CHAR(10), 'þ'), CHAR(13), ''), 'þ') AS LIST_DATA;


--
-- Use staging table
--

-- Clear data
TRUNCATE TABLE [STAGE].[STOCKS];

-- Load data
BULK INSERT [STAGE].[STOCKS]
FROM 'NEW/AAP-FY2017.CSV'
WITH 
(  
DATA_SOURCE = 'EDS_AZURE_4_STOCKS',
FORMAT = 'CSV', 
CODEPAGE = 65001, 
FIRSTROW = 2,
TABLOCK
); 

-- Show data
SELECT * FROM [STAGE].[STOCKS];


--
-- Use auditing table
--

-- Insert test record
INSERT INTO [STAGE].[AUDIT] (AU_CMD_TEXT) VALUES ('TEST')
GO

-- View test record
SELECT * FROM [STAGE].[AUDIT] 
GO

-- Clear tables
TRUNCATE TABLE [STAGE].[AUDIT];
TRUNCATE TABLE [STAGE].[STOCKS];


--
-- Load from blob storage
--

-- Drop stored procedure
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[LOAD_FROM_BLOB_STORAGE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [ACTIVE].[LOAD_FROM_BLOB_STORAGE]
GO
 

-- Create stored procedure
CREATE PROCEDURE [ACTIVE].[LOAD_FROM_BLOB_STORAGE]
    @VAR_VERBOSE_FLAG CHAR(1) = 'N'
AS
BEGIN

    -- Error handling variables
    DECLARE @VAR_ERR_NUM INT;
    DECLARE @VAR_ERR_LINE INT;
    DECLARE @VAR_ERR_MSG VARCHAR(1024);
    DECLARE @VAR_ERR_PROC VARCHAR(1024);

	-- Declare variables
    DECLARE @VAR_NEXT_FILE VARCHAR(256);
    DECLARE @VAR_AZURE_BLOB VARCHAR(256);
    DECLARE @VAR_SQL_STMT NVARCHAR(1024);
    DECLARE @VAR_FILE_CNT INT = 0;
 
    -- No counting of rows
    SET NOCOUNT ON;
  
	-- Debugging
    IF (@VAR_VERBOSE_FLAG = 'Y')
    BEGIN
        PRINT '[LOAD_FROM_BLOB_STORAGE] - STARTING TO EXECUTE STORED PROCEDURE.'
        PRINT ' '
    END

    -- ** ERROR HANDLING - START TRY **
    BEGIN TRY

	    -- Clear data
        TRUNCATE TABLE [STAGE].[STOCKS];

        -- Define cursor
        DECLARE VAR_FILE_CURSOR CURSOR FOR
            SELECT 
                CAST(LIST_DATA.VALUE AS VARCHAR(256)) AS PKG_LIST
            FROM 
                OPENROWSET
                (
                    BULK 'NEW/PACKING-LIST.TXT',
                    DATA_SOURCE = 'EDS_AZURE_4_STOCKS', 
                    SINGLE_CLOB
                ) AS RAW_DATA
            CROSS APPLY 
                STRING_SPLIT(REPLACE(REPLACE(RAW_DATA.BulkColumn, CHAR(10), 'þ'), CHAR(13), ''), 'þ') AS LIST_DATA
			WHERE
			    LTRIM(RTRIM(LIST_DATA.VALUE)) <> '';

        -- Open cursor
        OPEN VAR_FILE_CURSOR;

        -- Get first row
        FETCH NEXT FROM VAR_FILE_CURSOR INTO @VAR_NEXT_FILE;
		SET @VAR_AZURE_BLOB = CHAR(39) + 'NEW/' + @VAR_NEXT_FILE + CHAR(39);

        -- While there is data
        WHILE (@@fetch_status = 0)
        BEGIN

			-- Debugging
            IF (@VAR_VERBOSE_FLAG = 'Y')
            BEGIN
                PRINT '[LOAD_FROM_BLOB_STORAGE] - LOADING FILE ' + @VAR_AZURE_BLOB + '.'
                PRINT ' '
            END

            -- Create dynamic sql statement
            SELECT @VAR_SQL_STMT = '  
            BULK INSERT [STAGE].[STOCKS]  
            FROM ' + @VAR_AZURE_BLOB + '  
            WITH  
            (    
                DATA_SOURCE = ''EDS_AZURE_4_STOCKS'',  
                FORMAT = ''CSV'',  
                CODEPAGE = 65001,  
                FIRSTROW = 2,  
                TABLOCK  
            );'  

			-- Debugging
            IF (@VAR_VERBOSE_FLAG = 'Y')
            BEGIN			
			    PRINT @VAR_SQL_STMT
                PRINT ' '
            END

			-- Execute Bulk Insert
      	    EXEC SP_EXECUTESQL @VAR_SQL_STMT;

			-- Insert test record
            INSERT INTO [STAGE].[AUDIT] (AU_CMD_TEXT) VALUES (@VAR_SQL_STMT);

			-- Increment count
			SET @VAR_FILE_CNT += 1;

            -- Grab the next record
            FETCH NEXT FROM VAR_FILE_CURSOR INTO @VAR_NEXT_FILE;
    		SET @VAR_AZURE_BLOB = CHAR(39) + 'NEW/' + @VAR_NEXT_FILE + CHAR(39);

        END

	    -- Debugging
        IF (@VAR_VERBOSE_FLAG = 'Y')
        BEGIN
            PRINT '[LOAD_FROM_BLOB_STORAGE] - ENDING TO EXECUTE STORED PROCEDURE.'
            PRINT ' '
        END

        -- Close cursor
        CLOSE VAR_FILE_CURSOR;

        -- Release memory
        DEALLOCATE VAR_FILE_CURSOR;

		-- Return number of files
		RETURN @VAR_FILE_CNT;

    -- ** ERROR HANDLING - END TRY **
    END TRY

    -- ** Error Handling - Begin Catch **
    BEGIN CATCH
       
      -- Grab variables 
      SELECT 
          @VAR_ERR_NUM = ERROR_NUMBER(), 
          @VAR_ERR_PROC = ERROR_PROCEDURE(),
          @VAR_ERR_LINE = ERROR_LINE(), 
          @VAR_ERR_MSG = ERROR_MESSAGE();

      -- Raise error
      RAISERROR ('An error occurred within a user transaction. 
                  Error Number        : %d
                  Error Message       : %s  
                  Affected Procedure  : %s
                  Affected Line Number: %d'
                  , 16, 1
                  , @VAR_ERR_NUM, @VAR_ERR_MSG, @VAR_ERR_PROC, @VAR_ERR_LINE);       

	  -- Return number of files
	  RETURN -1;

    -- ** Error Handling - End Catch **    
    END CATCH                
            
END
GO


--
-- Test load process
--

-- Import blob files into stage
DECLARE @VARCNT INT;
EXEC @VARCNT = [ACTIVE].[LOAD_FROM_BLOB_STORAGE] @VAR_VERBOSE_FLAG = 'Y';
SELECT @VARCNT
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


-- Top 9 are newly started in 2013?
SELECT 
    *
FROM 
    [ACTIVE].[STOCKS]
WHERE
	ST_SYMBOL IN 
	(
	'HLT',
    'ALLE',
    'EVHC',
    'NWS',
    'NWSA',
    'COTY',
    'IQV',
    'ZTS'
    )
ORDER BY
    ST_DATE,
	ST_SYMBOL;
GO


-- Dividend records to be purged
SELECT 
    *
FROM 
    [ACTIVE].[STOCKS]
WHERE
	ST_OPEN = 0 AND ST_CLOSE = 0;
GO


-- Record count by year
SELECT
    YEAR(ST_DATE) AS MY_YEAR,
	COUNT(ST_DATE) AS MY_DATE 
FROM 
    ACTIVE.STOCKS
GROUP BY
    YEAR(ST_DATE) 


-- Make a backup of final table
-- SELECT * INTO ACTIVE.BAK_STOCKS FROM ACTIVE.STOCKS