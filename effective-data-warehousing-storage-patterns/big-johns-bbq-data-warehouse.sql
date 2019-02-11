/******************************************************
 *
 * Name:         big-johns-bbq-data-warehouse.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Create a large table of customers with
 *               1 million rows of data.
 *
 *     Schemas:  Staging tables are used to hold pieces 
 *               of data such as common first names.
 *               Fact table contains our main customer data.
 *               Dimension tables are for date & pork parts 
 *               Packages.
 *
 ******************************************************/


--
-- 1 - Create Bbq Data Warehouse
--

-- Which database to use.
USE [master]
GO

-- Delete existing databases.
/*
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'BIG_JONS_BBQ_DW')
    DROP DATABASE [BIG_JONS_BBQ_DW]
GO
*/

-- Add new databases.
CREATE DATABASE [BIG_JONS_BBQ_DW] ON
PRIMARY
    ( NAME = N'BIG_JONS_BBQ_DW_DAT', FILENAME = N'C:\MSSQL\DATA\BIG_JONS_BBQ_DW.MDF' ,
    SIZE = 512MB , MAXSIZE = UNLIMITED, FILEGROWTH = 64MB)
LOG ON
    ( NAME = N'BIG_JONS_BBQ_DW_LOG', FILENAME = N'C:\MSSQL\LOG\BIG_JONS_BBQ_DW.LDF' ,
    SIZE = 64MB , MAXSIZE = UNLIMITED, FILEGROWTH = 8MB );
GO 

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::BIG_JONS_BBQ_DW TO SA;
GO

-- Show the new database
SELECT * FROM sys.databases WHERE name = 'BIG_JONS_BBQ_DW';
GO



--
-- 2 - Create Fact schema
--

-- Which database to use.
USE [BIG_JONS_BBQ_DW]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'FACT')
DROP SCHEMA [FACT]
GO

-- Add new schema.
CREATE SCHEMA [FACT] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'FACT';
GO



--
-- 3 - Create Dimension schema
--

-- Which database to use.
USE [BIG_JONS_BBQ_DW]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'DIM')
DROP SCHEMA [DIM]
GO

-- Add new schema.
CREATE SCHEMA [DIM] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'DIM';
GO



--
-- 4 - Create stage schema
--

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'STAGE')
DROP SCHEMA [STAGE]
GO

-- Add new schema.
CREATE SCHEMA [STAGE] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'STAGE';
GO



--
-- 5 - Create date dim table 
--

-- Use the correct database
USE [BIG_JONS_BBQ_DW];
GO

-- Delete existing table
IF  OBJECT_ID(N'[DIM].[DIM_DATE]') > 0
    DROP TABLE [DIM].[DIM_DATE]
GO

-- Create new table
CREATE TABLE [DIM].[DIM_DATE]
(
  date_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_DIM_DATE PRIMARY KEY,
  date_date SMALLDATETIME NOT NULL,
  date_string VARCHAR(10) NOT NULL,
  date_month VARCHAR(3) NOT NULL,
  date_day VARCHAR(3) NOT NULL,
  date_int_qtr INT NOT NULL,
  date_int_doy INT NOT NULL,
  date_int_yyyy INT NOT NULL,
  date_int_mm INT NOT NULL,
  date_int_dd INT NOT NULL
);
GO

-- Drop existing index
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'IX_DIM_DATE_STR')
DROP INDEX IX_DIM_DATE_STR ON [DIM].[DIM_DATE];
GO

-- Create new index
CREATE INDEX IX_DIM_DATE_STR
    ON [DIM].[DIM_DATE] (date_string); 
GO



--
-- 6 - Load date dim table 
--

-- Create and intialize loop variable
DECLARE @VAR_DATE SMALLDATETIME;
SELECT @VAR_DATE = '2011-01-01';

-- Add data to our new table
WHILE (DATEDIFF(D, @VAR_DATE, '2013-12-31') <> 0)
BEGIN

    -- Add row to dimension table
    INSERT INTO [DIM].[DIM_DATE]
    (
        date_date,
        date_string,
        date_month,
        date_day,
        date_int_qtr,
        date_int_doy,
        date_int_yyyy,
        date_int_mm,
        date_int_dd
    )
    VALUES
	(
	   -- As small date time
	   @VAR_DATE,

	   -- Date as string
	   SUBSTRING(CONVERT(CHAR(10), @VAR_DATE, 120) + REPLICATE(' ', 10), 1, 10), 

	   -- Month as string
	   UPPER(SUBSTRING(CONVERT(CHAR(10), @VAR_DATE, 100) + REPLICATE(' ', 3), 1, 3)),

	   -- Day as string
	   UPPER(SUBSTRING(DATENAME(DW, @VAR_DATE), 1, 3)),

	   -- As quarter of year
	   DATEPART(QQ, @VAR_DATE),

	   -- As day of year
	   DATEPART(DY, @VAR_DATE),

	   -- Year as integer
	   YEAR(@VAR_DATE),

	   -- Month as integer
	   MONTH(@VAR_DATE),

	   -- Day as integer
	   DAY(@VAR_DATE)

    );

    -- Increment the counter
    SELECT @VAR_DATE = DATEADD(D, 1, @VAR_DATE);
END;



--
-- 7 - Create user defined functions
--

-- Make a year + quarter hash
CREATE FUNCTION [DIM].[UFN_GET_DATE_HASH] (@VAR_DATE_STR VARCHAR(10))
  RETURNS INT
BEGIN
  RETURN 
  (
      SELECT [date_int_yyyy] * 1000 + [date_int_qtr] 
	  FROM [DIM].[DIM_DATE] WHERE [date_string] = @VAR_DATE_STR
  )
END 
GO

-- Given a string, returnt the key
CREATE FUNCTION [DIM].[UFN_GET_DATE_KEY] (@VAR_DATE_STR VARCHAR(10))
  RETURNS INT
BEGIN
  RETURN 
  (
      SELECT date_key
	  FROM [DIM].[DIM_DATE] WHERE [date_string] = @VAR_DATE_STR
  )
END 
GO



--
-- 8 - Create zip code table
--

-- http://www.unitedstateszipcodes.org/zip-code-database/


-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[STAGE].[USPS_CITY_STATE_ZIP]') AND
    type in (N'U'))
DROP TABLE [STAGE].[USPS_CITY_STATE_ZIP]
GO

-- Create new table
CREATE TABLE [STAGE].[USPS_CITY_STATE_ZIP]
(
   csz_id int not null,
   csz_zip char(5) not null,
   csz_type varchar(128),
   csz_city varchar(128),
   csz_state char(2),
   csz_areacode varchar(128),
   csz_latitude decimal,
   csz_longitude decimal,
   csz_country char(2)
);
GO

-- Alter the table (primary key)
ALTER TABLE [STAGE].[USPS_CITY_STATE_ZIP]
   ADD CONSTRAINT PK_USPS_CITY_STATE_ZIP_ID PRIMARY KEY CLUSTERED (csz_id);
GO

-- Import data (42522 rows)
BULK INSERT [STAGE].[USPS_CITY_STATE_ZIP]
   FROM 'C:\BBQ_DW\CITY_STATE_ZIP.TXT'
   WITH
   (
       FIRSTROW = 2,
       FIELDTERMINATOR = '\t',
       ROWTERMINATOR = '\n',
       MAXERRORS = 10
   );
GO 

-- Remove quotes from area code
UPDATE [STAGE].[USPS_CITY_STATE_ZIP]
SET [csz_areacode] = REPLACE(ISNULL([csz_areacode], ''), '"', '');
GO

-- Show top 100 sample records
SELECT TOP 100 *
FROM [BIG_JONS_BBQ_DW].[STAGE].[USPS_CITY_STATE_ZIP];
GO



--
-- 9 - Create first names table
--

-- http://baby-names.familyeducation.com/popular-names/girls/
-- http://baby-names.familyeducation.com/popular-names/boys/

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[STAGE].[COMMON_FIRST_NAMES]') AND
    type in (N'U'))
DROP TABLE [STAGE].[COMMON_FIRST_NAMES]
GO

-- Create new table
CREATE TABLE [STAGE].[COMMON_FIRST_NAMES]
(
   cfn_id int not null,
   cfn_gender char(1),
   cfn_fname varchar(40)
);
GO

-- Alter the table (primary key)
ALTER TABLE [STAGE].[COMMON_FIRST_NAMES]
   ADD CONSTRAINT PK_COMMON_FIRST_NAMES_ID PRIMARY KEY CLUSTERED (cfn_id);
GO

-- Import data (200 rows)
BULK INSERT [STAGE].[COMMON_FIRST_NAMES]
   FROM 'C:\BBQ_DW\FIRST_NAMES.TXT'
   WITH
   (
       FIRSTROW = 1,
       FIELDTERMINATOR = '\t',
       ROWTERMINATOR = '\n',
       MAXERRORS = 10
   );
GO 

-- Show all records
SELECT *
FROM [BIG_JONS_BBQ_DW].[STAGE].[COMMON_FIRST_NAMES];
GO



--
-- 10 - Create last names table
--

-- http://names.mongabay.com/most_common_surnames.htm

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[STAGE].[COMMON_LAST_NAMES]') AND
    type in (N'U'))
DROP TABLE [STAGE].[COMMON_LAST_NAMES]
GO

-- Create new table
CREATE TABLE [STAGE].[COMMON_LAST_NAMES]
(
   cln_lname varchar(40),
   cln_count bigint,
   cln_pct real,
   cln_id int not null,
);
GO

-- Alter the table (primary key)
ALTER TABLE [STAGE].[COMMON_LAST_NAMES]
   ADD CONSTRAINT PK_COMMON_LAST_NAMES_ID PRIMARY KEY CLUSTERED (cln_id);
GO

-- Import data (1000 rows)
BULK INSERT [STAGE].[COMMON_LAST_NAMES]
   FROM 'C:\BBQ_DW\LAST_NAMES.TXT'
   WITH
   (
       FIRSTROW = 2,
       FIELDTERMINATOR = '\t',
       ROWTERMINATOR = '\n',
       MAXERRORS = 10
   );
GO 

-- Show all records
SELECT *
FROM [BIG_JONS_BBQ_DW].[STAGE].[COMMON_LAST_NAMES];
GO



--
-- 11 - Create street names table
--

-- http://www.livingplaces.com/streets/most-popular_street_names.html

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[STAGE].[COMMON_STREET_NAMES]') AND
    type in (N'U'))
DROP TABLE [STAGE].[COMMON_STREET_NAMES]
GO

-- Create new table
CREATE TABLE [STAGE].[COMMON_STREET_NAMES]
(
   csn_count int,
   csn_sname varchar(40)
);
GO

-- Import data (32154 rows)
BULK INSERT [STAGE].[COMMON_STREET_NAMES]
   FROM 'C:\BBQ_DW\STREET_NAMES.TXT'
   WITH
   (
       FIRSTROW = 1,
       FIELDTERMINATOR = '\t',
       ROWTERMINATOR = '\n',
       MAXERRORS = 10
   );
GO 

-- Alter the table (identity)
ALTER TABLE [STAGE].[COMMON_STREET_NAMES]
   ADD csn_id INT NOT NULL IDENTITY (1, 1)
GO

-- Alter the table (primary key)
ALTER TABLE [STAGE].[COMMON_STREET_NAMES]
   ADD CONSTRAINT PK_COMMON_STREET_NAMES_ID PRIMARY KEY CLUSTERED (csn_id);
GO

-- Show all records
SELECT *
FROM [BIG_JONS_BBQ_DW].[STAGE].[COMMON_STREET_NAMES];
GO


--
-- 12 - Create pig packages
--


-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DIM].[PIG_PACKAGES]') AND
    type in (N'U'))
DROP TABLE [DIM].[PIG_PACKAGES]
GO

-- Create new table
CREATE TABLE [DIM].[PIG_PACKAGES]
(
   pkg_id int not null identity (1, 1),
   pkg_name varchar(40)
);
GO

-- Alter the table (primary key)
ALTER TABLE [DIM].[PIG_PACKAGES]
   ADD CONSTRAINT PK_PIG_PACKAGES_ID PRIMARY KEY CLUSTERED (pkg_id);
GO

-- Add data to the table
INSERT INTO [DIM].[PIG_PACKAGES] (pkg_name)
VALUES
   ('Baby Back Ribs'),
   ('Bacon'),
   ('Ham'),
   ('Hock'),
   ('Jowl Bacon'),
   ('Lion Chops'),
   ('Neck Chop'),
   ('Picnic Shoulder'),
   ('Pork Butt'),
   ('Spare Ribs');

-- Show all records
SELECT *
FROM [BIG_JONS_BBQ_DW].[DIM].[PIG_PACKAGES];
GO


--
-- 13 - Create a sequence object
--

-- Delete existing sequence.
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'SEQ_CUSTOMERS')
    DROP SEQUENCE [FACT].[SEQ_CUSTOMERS]
GO

-- Add new sequence.
CREATE SEQUENCE [FACT].[SEQ_CUSTOMERS]
    AS INT
    START WITH 100
    INCREMENT BY 1
    MINVALUE 1
    NO MAXVALUE 
    NO CYCLE
    NO CACHE;
GO

-- Alter the sequence.
ALTER SEQUENCE [FACT].[SEQ_CUSTOMERS] RESTART WITH 1;
GO

-- Show the new sequence
SELECT * FROM sys.sequences WHERE name = 'SEQ_CUSTOMERS' ;
GO


--
-- 14 - Create customers fact table 
--

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[FACT].[CUSTOMERS]') AND
    type in (N'U'))
DROP TABLE [FACT].[CUSTOMERS]
GO

-- Create new table
CREATE TABLE [FACT].[CUSTOMERS]
(
   cus_id int not null,
   cus_lname varchar(40),
   cus_fname varchar(40),
   cus_phone char(12),
   cus_address varchar(40),
   cus_city varchar(20),
   cus_state char(2),
   cus_zip char(5) not null,
   cus_package_key int not null,
   cus_start_date_key int not null,
   cus_end_date_key int not null,
   cus_date_str varchar(10) not null,
   cus_qtr_key as [DIM].[UFN_GET_DATE_HASH] (cus_date_str) 
);
GO


-- Alter the customers table (primary key)
ALTER TABLE [FACT].[CUSTOMERS] 
   ADD CONSTRAINT PK_CUSTOMERS_ID PRIMARY KEY CLUSTERED (cus_id);
GO

-- Alter the customers table (foreign key - package key)
ALTER TABLE [FACT].[CUSTOMERS] 
ADD CONSTRAINT FK_DIM_PIG_PACKAGES FOREIGN KEY (cus_package_key)
    REFERENCES [DIM].[PIG_PACKAGES] (pkg_id) ;

-- Alter the customers table (foreign key - start date key)
ALTER TABLE [FACT].[CUSTOMERS] 
ADD CONSTRAINT FK_DIM_START_DATE FOREIGN KEY (cus_start_date_key)
    REFERENCES [DIM].[DIM_DATE] (date_key) ;

-- Alter the customers table (foreign key - start date key)
ALTER TABLE [FACT].[CUSTOMERS] 
ADD CONSTRAINT FK_DIM_END_DATE FOREIGN KEY (cus_end_date_key)
    REFERENCES [DIM].[DIM_DATE] (date_key) ;

-- Alter the customers table (default constraint - start date key)
ALTER TABLE [FACT].[CUSTOMERS] 
   ADD CONSTRAINT DF_START_DTE DEFAULT (1) FOR [cus_start_date_key];
GO

-- Alter the customers table (default constraint - end date key)
ALTER TABLE [FACT].[CUSTOMERS] 
   ADD CONSTRAINT DF_END_DTE DEFAULT (365) FOR [cus_end_date_key];
GO



--
-- 15 - MAIN PROGRAM - ADD 1M ROWS OF DATA (57:18 runtime)
--

-- Remove the data
TRUNCATE TABLE [FACT].[CUSTOMERS];
ALTER SEQUENCE [FACT].[SEQ_CUSTOMERS] RESTART WITH 1;
GO

-- Declare local variables
DECLARE @VAR_CNT int = 1;
DECLARE @VAR_RND float = 0.0;
DECLARE @VAR_ID int = 0;

-- Declare column variables
DECLARE @VAR_LNAME VARCHAR(40) = '';
DECLARE @VAR_FNAME VARCHAR(40) = '';
DECLARE @VAR_PHONE VARCHAR(12) = '';
DECLARE @VAR_ADDRESS VARCHAR(40) = '';
DECLARE @VAR_AREA_CODE VARCHAR(128) = '';
DECLARE @VAR_TEMP_CODE VARCHAR(7) = '';

DECLARE @VAR_CITY VARCHAR(20) = 'COVENTRY';
DECLARE @VAR_ZIP CHAR(5) = '02816';
DECLARE @VAR_STATE CHAR(2) = 'RI';
DECLARE @VAR_COUNTRY CHAR(2) = 'US';
DECLARE @VAR_PACKAGE INT = 0;

DECLARE @VAR_START_DTE smalldatetime = null;
DECLARE @VAR_END_DTE smalldatetime = null;
DECLARE @VAR_START_KEY int = 1;
DECLARE @VAR_END_KEY int = 365;


-- Turn off counting
SET NOCOUNT ON

-- Add a 1 million rows
WHILE (@VAR_CNT < 1000000)
BEGIN

   -- Show the counter
   IF (SELECT @VAR_CNT % 1000) = 1
       PRINT @VAR_CNT;

   -- Get a random number
   SELECT @VAR_RND = RAND();

   -- Get the next id
   SELECT @VAR_ID = NEXT VALUE FOR [FACT].[SEQ_CUSTOMERS];

   -- A - Get city, state, zip code, country
   SELECT
        @VAR_CITY = [csz_city]
     ,  @VAR_ZIP = [csz_zip]
     ,  @VAR_STATE = [csz_state]
     ,  @VAR_COUNTRY = [csz_country]
     ,  @VAR_AREA_CODE = [csz_areacode]
   FROM
        [STAGE].[USPS_CITY_STATE_ZIP] 
   WHERE
        [csz_id] = CAST(@VAR_RND * (42522 + 1) AS INT);


   -- B - Get pig parts packages
   SELECT @VAR_PACKAGE = CAST(@VAR_RND *  (10) AS INT) + 1;


   -- C - Get first name
   SELECT
        @VAR_FNAME = [cfn_fname]
   FROM
        [STAGE].[COMMON_FIRST_NAMES]
    WHERE
	[cfn_id] = CAST(@VAR_RND * (200 + 1) AS INT);


   -- D - Get last name
   SELECT
        @VAR_LNAME = UPPER(SUBSTRING([cln_lname], 1, 1)) + LOWER(SUBSTRING([cln_lname], 2, LEN([cln_lname])-1))
   FROM
        [STAGE].[COMMON_LAST_NAMES]
   WHERE
        [cln_id] = CAST(@VAR_RND * (1000 + 1) AS INT);


   -- E - Get address
   SELECT
        @VAR_ADDRESS = [csn_sname]
   FROM
        [STAGE].[COMMON_STREET_NAMES]
   WHERE
        [csn_id] = CAST(@VAR_RND * (32154 + 1) AS INT);


   -- F - Customers contract date (start key / end key)
   SELECT @VAR_START_DTE = '2011-01-01';
   SELECT @VAR_START_DTE = DATEADD(DD, CAST(@VAR_RND * (365 * 2) AS INT), @VAR_START_DTE);
   SELECT @VAR_START_KEY = [DIM].[UFN_GET_DATE_KEY] (CONVERT(CHAR(10), @VAR_START_DTE, 120));
   SELECT @VAR_END_KEY = @VAR_START_KEY + 364;


   -- G - Create an area code based off location and random number
   SELECT @VAR_TEMP_CODE = STR(CAST(RAND() * 9999999 AS INT), 7, 0);
   SELECT @VAR_AREA_CODE =
       CASE
           WHEN LEN(@VAR_AREA_CODE) < 3 THEN SPACE(12)
	   ELSE REPLACE(SUBSTRING(@VAR_AREA_CODE, 1, 3) + '-' + SUBSTRING(@VAR_TEMP_CODE, 1, 3) + '-' + SUBSTRING(@VAR_TEMP_CODE, 4, 4), ' ', '0')
       END;


   -- Add new records
   INSERT INTO [FACT].[CUSTOMERS] 
   (
        [cus_id]
     ,  [cus_lname]
     ,  [cus_fname]
     ,  [cus_phone]
     ,  [cus_address]
     ,  [cus_city]
     ,  [cus_state]
     ,  [cus_zip]
     ,  [cus_package_key]
     ,  [cus_start_date_key]
     ,  [cus_end_date_key]
	 ,  [cus_date_str]
   )
   VALUES
   (
        @VAR_ID    
	 ,  @VAR_LNAME
	 ,  @VAR_FNAME
	 ,  @VAR_AREA_CODE
	 ,  @VAR_ADDRESS
	 ,  @VAR_CITY 
	 ,  @VAR_STATE
     ,  @VAR_ZIP 
	 ,  @VAR_PACKAGE
	 ,  @VAR_START_KEY
	 ,  @VAR_END_KEY
	 ,  CONVERT(CHAR(10), @VAR_START_DTE, 120)
   );
	    
   -- Increment the counter
   SET @VAR_CNT = @VAR_CNT + 1;

END

-- Turn on counting
SET NOCOUNT ON
GO



--
-- 16 - Show the distribution of the data
--

SELECT 
  c.cus_qtr_key, 
  count(*) as cus_total 
FROM 
  [FACT].[CUSTOMERS] c
GROUP BY 
  c.cus_qtr_key
ORDER BY 
  c.cus_qtr_key


/*

cus_qtr_key	cus_total
2011001	123032
2011002	124689
2011003	126569
2011004	126073
2012001	124540
2012002	124341
2012003	126093
2012004	124662

*/
