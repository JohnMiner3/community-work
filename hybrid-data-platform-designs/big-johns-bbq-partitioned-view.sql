/******************************************************
 *
 * Name:         big-johns-bbq-partitioned-view.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Convert the Big Jons BBQ data warehouse
 *               into a partitioned view example.
 *
 ******************************************************/

 
--
-- 1 - Create primary database to house Dimension tables
--

-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'BBQ_PART_VIEW')
    DROP DATABASE [BBQ_PART_VIEW]
GO

-- Add new databases
CREATE DATABASE [BBQ_PART_VIEW] ON
PRIMARY
    ( NAME = N'BBQ_PART_VIEW_DAT', FILENAME = N'C:\MSSQL\DATA\BBQ_PART_VIEW_DW.MDF' ,
    SIZE = 4MB , MAXSIZE = UNLIMITED, FILEGROWTH = 4MB)
LOG ON
    ( NAME = N'BBQ_PART_VIEW_LOG', FILENAME = N'C:\MSSQL\LOG\BBQ_PART_VIEW_DW.LDF' ,
    SIZE = 4MB , MAXSIZE = UNLIMITED, FILEGROWTH = 4MB );
GO 

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::BBQ_PART_VIEW TO SA;
GO

-- Show the new database
SELECT * FROM sys.databases WHERE name = 'BBQ_PART_VIEW';
GO


--
-- 2 - Create Fact schema
--

-- Which database to use.
USE [BBQ_PART_VIEW]
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
USE [BBQ_PART_VIEW]
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
-- 4 - Create date dim table 
--

-- Use the correct database
USE [BBQ_PART_VIEW];
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
-- 5 - Create user defined functions
--

-- Use the correct database
USE [BBQ_PART_VIEW];
GO

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



--
-- 6 - Create pig packages
--

-- Use the correct database
USE [BBQ_PART_VIEW];
GO

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



--
-- 7 - Load Pig Packages - Dimension Table
--

-- Use the correct database
USE [BBQ_PART_VIEW];
GO

-- Add the data to the table
INSERT INTO [DIM].[PIG_PACKAGES] ([pkg_name]) 
  SELECT ([pkg_name]) FROM [BIG_JONS_BBQ_DW].[DIM].[PIG_PACKAGES] WITH (NOLOCK);
  


--
-- 8 - Load Date - Dimension Table
--

-- Use the correct database
USE [BBQ_PART_VIEW];
GO

-- Add the data to the table
INSERT INTO [DIM].[DIM_DATE] 
( 
    [date_date]
    ,[date_string]    
    ,[date_month]    
    ,[date_day]    
    ,[date_int_qtr]    
    ,[date_int_doy]    
    ,[date_int_yyyy]    
    ,[date_int_mm]    
    ,[date_int_dd]    
)    
SELECT     
     [date_date]    
    ,[date_string]    
    ,[date_month]    
    ,[date_day]    
    ,[date_int_qtr]    
    ,[date_int_doy]    
    ,[date_int_yyyy]    
    ,[date_int_mm]    
    ,[date_int_dd]    
FROM [BIG_JONS_BBQ_DW].[DIM].[DIM_DATE] WITH (NOLOCK);    



--
-- 9 - Make eight files, eight file groups, eight tables w/constraints, & load with data
-- 

-- Declare local variables
DECLARE @VAR_YEAR1 INT = 2011;
DECLARE @VAR_QTR1 INT = 1;
DECLARE @VAR_TAG1 CHAR(7) = '2011001';
DECLARE @VAR_NAME1 VARCHAR(64);
DECLARE @VAR_STMT1 VARCHAR(MAX);

-- For two years
WHILE (@VAR_YEAR1 < 2013)
BEGIN

    -- Set to one
	SET @VAR_QTR1 = 1;

    -- For four quarters
    WHILE (@VAR_QTR1 < 5)
    BEGIN

	   --  Make up the database name
	   SELECT @VAR_TAG1 = @VAR_YEAR1 * 1000 + @VAR_QTR1;
	   SET @VAR_NAME1 = 'PART_VIEW_' + @VAR_TAG1;


       -- A - Add file group    
       SELECT @VAR_STMT1 = 'ALTER DATABASE [BBQ_PART_VIEW] ADD FILEGROUP [FG_' +  @VAR_NAME1 + '];';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;
    

       -- B - Add the file  
       SELECT @VAR_STMT1 = 'ALTER DATABASE [BBQ_PART_VIEW] ADD FILE (NAME=FN_' + @VAR_NAME1 + 
                          ', FILENAME=''C:\MSSQL\DATA\FN_' +  @VAR_NAME1 + 
                          '.NDF'', SIZE=16MB, FILEGROWTH=4MB) TO FILEGROUP [FG_' +  @VAR_NAME1 + '];';
						          
       -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;


       -- C - Create Customer Fact Table
	   SET @VAR_STMT1 = '';
       SET @VAR_STMT1 = @VAR_STMT1 + 'USE [BBQ_PART_VIEW]; ';

	   SET @VAR_STMT1 = @VAR_STMT1 + 'CREATE TABLE [FACT].[CUSTOMERS_' +  @VAR_TAG1 + '] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '( ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_id int not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_lname varchar(40), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_fname varchar(40), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_phone char(12), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_address varchar(40), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_city varchar(20), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_state char(2), ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_zip char(5) not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_package_key int not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_start_date_key int not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_end_date_key int not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_date_str varchar(10) not null, ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '  cus_qtr_key int not null ';
	   SET @VAR_STMT1 = @VAR_STMT1 + ') ON [FG_' +  @VAR_NAME1 + '];';

       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ADD CONSTRAINT CHK_QTR_' + @VAR_TAG1 + '_KEY CHECK (cus_qtr_key = ' + @VAR_TAG1 + '); ';
	   SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ADD CONSTRAINT PK_CUST_' + @VAR_TAG1 + '_ID PRIMARY KEY CLUSTERED (cus_id, cus_qtr_key); ';
       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ADD CONSTRAINT DF_START_' + @VAR_TAG1 + '_DTE DEFAULT (1) FOR [cus_start_date_key]; ';
       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ADD CONSTRAINT DF_END_' + @VAR_TAG1 + '_DTE DEFAULT (365) FOR [cus_end_date_key]; ';

       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ';
       SET @VAR_STMT1 = @VAR_STMT1 + 'ADD CONSTRAINT FK_DIM_PIG_' + @VAR_TAG1 + '_PKGS FOREIGN KEY (cus_package_key) ';
       SET @VAR_STMT1 = @VAR_STMT1 + '    REFERENCES [DIM].[PIG_PACKAGES] (pkg_id); ';
	   
       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ';
       SET @VAR_STMT1 = @VAR_STMT1 + 'ADD CONSTRAINT FK_DIM_START_' + @VAR_TAG1 + '_DTE FOREIGN KEY (cus_start_date_key) ';
       SET @VAR_STMT1 = @VAR_STMT1 + '    REFERENCES [DIM].[DIM_DATE] (date_key); ';

       SET @VAR_STMT1 = @VAR_STMT1 + 'ALTER TABLE [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ';
       SET @VAR_STMT1 = @VAR_STMT1 + 'ADD CONSTRAINT FK_DIM_END_' + @VAR_TAG1 + '_DTE FOREIGN KEY (cus_end_date_key) ';
       SET @VAR_STMT1 = @VAR_STMT1 + '    REFERENCES [DIM].[DIM_DATE] (date_key); ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;


       -- D - Load Customer Fact Table
	   SET @VAR_STMT1 = '';
       SET @VAR_STMT1 = @VAR_STMT1 + 'USE [BBQ_PART_VIEW]; ';

	   SET @VAR_STMT1 = @VAR_STMT1 + 'INSERT INTO [FACT].[CUSTOMERS_' + @VAR_TAG1 + '] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '( ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '      [cus_id] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_lname] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_fname] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_phone] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_address] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_city] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_state] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_zip] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_package_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_start_date_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_end_date_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_date_str] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_qtr_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + ') ';

	   SET @VAR_STMT1 = @VAR_STMT1 + 'SELECT ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '      [cus_id] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_lname] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_fname] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_phone] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_address] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_city] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_state] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_zip] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_package_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_start_date_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_end_date_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_date_str] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + '     ,[cus_qtr_key] ';
	   SET @VAR_STMT1 = @VAR_STMT1 + 'FROM [BIG_JONS_BBQ_DW].[FACT].[CUSTOMERS] WITH (NOLOCK)  ';
	   SET @VAR_STMT1 = @VAR_STMT1 + 'WHERE [cus_qtr_key] = ' + CHAR(39) + @VAR_TAG1 + CHAR(39) + '; ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;


	   -- Increment the quarter
	   SET @VAR_QTR1 = @VAR_QTR1 + 1

	END

	-- Increment the year
	SET @VAR_YEAR1 = @VAR_YEAR1 + 1
END
GO



--
-- 10 - Create a view to bind them all
-- 

-- Declare local variables
DECLARE @VAR_YEAR2 INT = 2011;
DECLARE @VAR_QTR2 INT = 1;
DECLARE @VAR_TAG2 CHAR(7) = '2011001';
DECLARE @VAR_NAME2 VARCHAR(64);
DECLARE @VAR_STMT2 VARCHAR(MAX);

-- Initialize the variable
SET @VAR_STMT2 = 'USE [BBQ_PART_VIEW]; ' + CHAR(10);
SET @VAR_STMT2 = @VAR_STMT2 + 'EXECUTE (' + CHAR(39) + 'CREATE VIEW [FACT].[CUSTOMERS] WITH SCHEMABINDING AS ' + CHAR(10);

-- For two years
WHILE (@VAR_YEAR2 < 2013)
BEGIN

    -- Set to one
	SET @VAR_QTR2 = 1;

    -- For four quarters
    WHILE (@VAR_QTR2 < 5)
    BEGIN

	   --  Make up the database name
	   SELECT @VAR_TAG2 = @VAR_YEAR2 * 1000 + @VAR_QTR2;
	   SET @VAR_NAME2 = '[FACT].[CUSTOMERS_' + @VAR_TAG2 + ']';

	   -- Combine into one TSQL statement
	   SET @VAR_STMT2 = @VAR_STMT2 + ' SELECT ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '      [cus_id] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_lname] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_fname] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_phone] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_address] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_city] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_state] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_zip] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_package_key] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_start_date_key] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_end_date_key] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_date_str] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + '     ,[cus_qtr_key] ' + CHAR(10);
	   SET @VAR_STMT2 = @VAR_STMT2 + ' FROM ' + @VAR_NAME2 + CHAR(10) + ' UNION ALL ' + CHAR(10);

	   -- Increment the quarter
	   SET @VAR_QTR2 = @VAR_QTR2 + 1

	END

	-- Increment the year
	SET @VAR_YEAR2 = @VAR_YEAR2 + 1
END

-- Remove the last union all stmt
SELECT @VAR_STMT2 = SUBSTRING(@VAR_STMT2, 1, LEN(@VAR_STMT2) - 11);
SELECT @VAR_STMT2 = @VAR_STMT2 + CHAR(39) + ');';


-- Create the view with schema binding
EXECUTE (@VAR_STMT2);
PRINT @VAR_STMT2

GO