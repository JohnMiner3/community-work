--
-- Name:         nb-second-exploration
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  Second try at serverless SQL pools
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

USE mssqltips;
GO

--
-- 3 - Ad-hoc query of parquet file
-- 

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/parquet-files/DimCustomer',
        FORMAT = 'PARQUET'
    ) AS [result]


--
-- 4 - Ad-hoc query of csv file
-- 

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/csv-files/DimCustomer.csv',
        FORMAT = 'CSV',
		FIELDTERMINATOR = '|',
		PARSER_VERSION='2.0'
    ) AS [result]


--
-- 5 - Create external file formats
--

-- Delimited files
CREATE EXTERNAL FILE FORMAT [DelimitedFile] 
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

-- Parquet files
CREATE EXTERNAL FILE FORMAT [ParquetFile] 
WITH 
( 
	FORMAT_TYPE = PARQUET
)
GO

--
-- 6 - Create external data source
--

-- Depends on managed identity having access to adls gen2 account/container
CREATE EXTERNAL DATA SOURCE [LakeDataSource] 
WITH 
(
	LOCATION = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net' 
)
GO


--
-- 7 - Create external table
--


--DROP EXTERNAL TABLE dim_currency;
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
	FILE_FORMAT = [DelimitedFile]
)
GO


--
-- 8 - Query the table
--

SELECT TOP 10 * FROM dbo.dim_currency
GO


--
-- 9 - Create external table
--

-- DROP EXTERNAL TABLE fact_internet_sales
GO

CREATE EXTERNAL TABLE fact_internet_sales
(
	[CurrencyKey] int,
	[CurrencyAlternateKey] nvarchar(4000),
	[CurrencyName] nvarchar(4000)
)
WITH 
(
	LOCATION = 'synapse/parquet-files/FactInternetSales/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 10 - Query the table
--

SELECT TOP 10 * FROM dbo.fact_internet_sales
GO


--
-- 10 - Drop existing database
--

USE master;
GO

DROP DATABASE IF EXISTS mssqltips;
GO

