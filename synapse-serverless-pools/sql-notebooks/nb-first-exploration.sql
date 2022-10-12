--
-- Name:         nb-first-exploration
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  First try at serverless SQL pools
--


--
-- 0 - List logical databases
--

USE master;
GO

SELECT * FROM sys.databases;
GO

--
-- 1 - Drop existing database
--

DROP DATABASE IF EXISTS mssqltips;
GO


--
-- 2 - Create new database
--

CREATE DATABASE mssqltips;
GO


--
-- 3 - Create external file format
--

USE mssqltips;
GO

CREATE EXTERNAL FILE FORMAT [PipeDelimitedText] 
WITH 
( 
	FORMAT_TYPE = DELIMITEDTEXT ,
	FORMAT_OPTIONS 
	(
		FIELD_TERMINATOR = '|',
		USE_TYPE_DEFAULT = FALSE
	)
)
GO


--
-- 4 - Create external data source
--

-- Depends on managed identity having access to adls gen2 account/container
CREATE EXTERNAL DATA SOURCE [LakeDataSource] 
WITH 
(
	LOCATION = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net' 
)
GO


--
-- 5 - Create external table
--

DROP EXTERNAL TABLE dim_currency;
GO

CREATE EXTERNAL TABLE dim_currency
(
    CurrencyKey int,
	CurrencyAlternateKey nvarchar(3),
	CurrencyName nvarchar(50)
)
WITH 
(
	LOCATION = 'synapse/csv-files/DimCurrency.csv',
	DATA_SOURCE = [LakeDataSource],
	FILE_FORMAT = [PipeDelimitedText]
)
GO


--
-- 6 - Query the database
--

SELECT TOP 10 * FROM dbo.dim_currency
GO


--
-- 7 - Don't need logical database for interactive queries
--

SELECT R.*
FROM OPENROWSET (
    BULK 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimCurrency.csv',
    FORMAT = 'CSV',
	FIELDTERMINATOR = '|',
    HEADER_ROW = FALSE )
WITH (
    CurrencyKey int,
	CurrencyAlternateKey nvarchar(3),
	CurrencyName nvarchar(50)
) AS R;
GO	


--
-- 8 - Drop existing database
--

USE master;
GO

DROP DATABASE IF EXISTS mssqltips;
GO

