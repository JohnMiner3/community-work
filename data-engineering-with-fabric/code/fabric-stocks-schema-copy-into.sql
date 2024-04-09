
/******************************************************
 *
 * Name:     fabric-stocks-schema-copy-into.sql
 *   
 * Design Phase:
 *   Author:  John Miner
 *   Date:    03-15-2024
 *   Purpose: Create stocks schema, load csv data
 * 
 ******************************************************/


--
-- Create stocks schema
--

-- Delete existing schema.
DROP SCHEMA IF EXISTS [stocks]
GO
 
-- Add new schema.
CREATE SCHEMA [stocks] AUTHORIZATION [dbo]
GO


--
-- Create table 2013
--
 
-- Delete existing table
DROP TABLE IF EXISTS [stocks].[snp500]
GO
 
-- Create new table
CREATE TABLE [stocks].[snp500]
(
  ST_SYMBOL VARCHAR(32) NOT NULL,
  ST_DATE DATE NOT NULL,
  ST_OPEN REAL NULL,
  ST_HIGH REAL NULL,
  ST_LOW REAL NULL,
  ST_CLOSE REAL NULL,
  ST_ADJ_CLOSE REAL NULL,
  ST_VOLUME BIGINT NULL
);


--
-- Load table - all years - fails due to bug
--

COPY INTO [stocks].[snp500]
FROM
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks/S&P-2013/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks/S&P-2014/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks/S&P-2015/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks/S&P-2016/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks/S&P-2017/*.CSV'
WITH 
(
    FILE_TYPE = 'CSV',
    CREDENTIAL=(IDENTITY= 'Storage Account Key', SECRET='tA5k3pJRZv16vRQOOV80D7moaRLnE5rNF5gnjPU3YR6FrKcLz6uCHaaLfd9PN4mCk7BNvY6RsSyiFmQSd9MCwA=='),
    FIELDQUOTE = '"',
    FIELDTERMINATOR=',',
    ROWTERMINATOR='0x0A',
    ENCODING = 'UTF8',
    FIRSTROW = 2
)
GO



--
-- Load table - all years - fails due to bug
--

COPY INTO [stocks].[snp500]
FROM
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks2/YR2013/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks2/YR2014/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks2/YR2015/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks2/YR2016/*.CSV',
  'https://sa4adls2030.dfs.core.windows.net/raw/stocks2/YR2017/*.CSV'
WITH 
(
    FILE_TYPE = 'CSV',
    CREDENTIAL=(IDENTITY= 'Storage Account Key', SECRET='tA5k3pJRZv16vRQOOV80D7moaRLnE5rNF5gnjPU3YR6FrKcLz6uCHaaLfd9PN4mCk7BNvY6RsSyiFmQSd9MCwA=='),
    FIELDQUOTE = '"',
    FIELDTERMINATOR=',',
    ROWTERMINATOR='0x0A',
    ENCODING = 'UTF8',
    FIRSTROW = 2
)
GO



--
-- Show data
--

SELECT COUNT(*) FROM [stocks].[snp500] 
GO

SELECT TOP(25) * FROM [stocks].[snp500] 
GO

-- How is MSFT doing?
SELECT
  YEAR(ST_DATE) AS ST_YEAR,
  MONTH(ST_DATE) AS ST_MONTH,
  ST_SYMBOL,
  AVG(ST_CLOSE) AS AVG_PRICE,
  AVG(ST_VOLUME) AS AVG_VOLUME
FROM
  [stocks].[snp500]
WHERE
   ST_SYMBOL = 'MSFT'
GROUP BY
  YEAR(ST_DATE),
  MONTH(ST_DATE),
  ST_SYMBOL
ORDER BY
  YEAR(ST_DATE),
  MONTH(ST_DATE)
GO



--
-- Cleanup demo
--

-- delete existing table
DROP TABLE IF EXISTS [stocks].[snp500]
GO


-- Delete existing schema.
DROP SCHEMA IF EXISTS [stocks]
GO