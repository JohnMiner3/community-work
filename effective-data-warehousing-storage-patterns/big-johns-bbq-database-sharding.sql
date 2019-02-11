/******************************************************
 *
 * Name:         big-johns-bbq-database-sharding.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Convert the Big Jons BBQ data warehouse
 *               into a sharding example.
 *
 ******************************************************/


-- Declare local variables
DECLARE @VAR_YEAR INT = 2011;
DECLARE @VAR_QTR INT = 1;
DECLARE @VAR_TAG CHAR(7) = '2011001';
DECLARE @VAR_NAME VARCHAR(64);
DECLARE @VAR_STMT VARCHAR(MAX);

-- For two years
WHILE (@VAR_YEAR < 2013)
BEGIN

    -- Set to one
	SET @VAR_QTR = 1;

    -- For four quarters
    WHILE (@VAR_QTR < 5)
    BEGIN

	   --  Make up the database name
	   SELECT @VAR_TAG = @VAR_YEAR * 1000 + @VAR_QTR;
	   SET @VAR_NAME = 'BBQ_SHARD_' + @VAR_TAG;

	   PRINT @VAR_NAME

       --
       -- 1 - Delete existing databases
       --

       SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [master]; '
       SET @VAR_STMT = @VAR_STMT + 'IF EXISTS (SELECT name FROM sys.databases WHERE name = ' + CHAR(39) + @VAR_NAME + CHAR(39) +
       ') DROP DATABASE [' + @VAR_NAME + '];'

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 2 - Add new databases
       --
	  
	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [master]; '
       SET @VAR_STMT = @VAR_STMT + 'CREATE DATABASE [' + @VAR_NAME + '] ON PRIMARY ';
       SET @VAR_STMT = @VAR_STMT + '( NAME = N' + CHAR(39) + @VAR_NAME + '_DAT' + CHAR(39) + ', FILENAME = N' + CHAR(39) + 'C:\MSSQL\DATA\' + @VAR_NAME + '.MDF' + CHAR(39) + ', ';
       SET @VAR_STMT = @VAR_STMT + 'SIZE = 16MB , MAXSIZE = UNLIMITED, FILEGROWTH = 16MB) ';
       SET @VAR_STMT = @VAR_STMT + ' LOG ON ';
       SET @VAR_STMT = @VAR_STMT + '( NAME = N' + CHAR(39) + @VAR_NAME + '_LOG' + CHAR(39) + ', FILENAME = N' + CHAR(39) + 'C:\MSSQL\LOG\' + @VAR_NAME + '.LDF' + CHAR(39) + ', ';
       SET @VAR_STMT = @VAR_STMT + 'SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 8MB );';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 3 - Switch owner to system admin
       --
	   	  
	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [master]; ';
       SET @VAR_STMT = @VAR_STMT + 'ALTER AUTHORIZATION ON DATABASE::' + @VAR_NAME + ' TO SA;';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 4 - Create Fact Schema
       --
	   	  
	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
       SET @VAR_STMT = @VAR_STMT + 'EXECUTE (' + CHAR(39) + 'CREATE SCHEMA [FACT] AUTHORIZATION [dbo];' + CHAR(39) + '); '; 

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 5 - Create Dim Schema
       --
	   	  
	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
       SET @VAR_STMT = @VAR_STMT + 'EXECUTE (' + CHAR(39) + 'CREATE SCHEMA [DIM] AUTHORIZATION [dbo];' + CHAR(39) + '); '; 

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 6 - Create Pig Packages - Dimension Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'CREATE TABLE [DIM].[PIG_PACKAGES] ';
	   SET @VAR_STMT = @VAR_STMT + '( ';
	   SET @VAR_STMT = @VAR_STMT + '   pkg_id INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PIG_PACKAGES_ID PRIMARY KEY, ';
	   SET @VAR_STMT = @VAR_STMT + '   pkg_name VARCHAR(40) ';
	   SET @VAR_STMT = @VAR_STMT + '); ';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 7 - Create Date - Dimension Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'CREATE TABLE [DIM].[DIM_DATE] ';
	   SET @VAR_STMT = @VAR_STMT + '( ';
	   SET @VAR_STMT = @VAR_STMT + '  date_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_DIM_DATE PRIMARY KEY, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_date SMALLDATETIME NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_string VARCHAR(10) NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_month VARCHAR(3) NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_day VARCHAR(3) NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_int_qtr INT NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_int_doy INT NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_int_yyyy INT NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_int_mm INT NOT NULL, ';
	   SET @VAR_STMT = @VAR_STMT + '  date_int_dd INT NOT NULL ';
	   SET @VAR_STMT = @VAR_STMT + '); ';	   
	   SET @VAR_STMT = @VAR_STMT + 'CREATE INDEX [IX_DIM_DATE_STR] ON [DIM].[DIM_DATE] (date_string); ';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 8 - Create User Defined Hash Function
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'EXECUTE (' + CHAR(39);
	   SET @VAR_STMT = @VAR_STMT + '  CREATE FUNCTION [DIM].[UFN_GET_DATE_HASH] (@VAR_DATE_STR VARCHAR(10)) ';
	   SET @VAR_STMT = @VAR_STMT + '    RETURNS INT ';
	   SET @VAR_STMT = @VAR_STMT + '  BEGIN ';
	   SET @VAR_STMT = @VAR_STMT + '    RETURN '; 
	   SET @VAR_STMT = @VAR_STMT + '    ( ';
       SET @VAR_STMT = @VAR_STMT + '    SELECT [date_int_yyyy] * 1000 + [date_int_qtr] ';
	   SET @VAR_STMT = @VAR_STMT + '    FROM [DIM].[DIM_DATE] WHERE [date_string] = @VAR_DATE_STR ';
  	   SET @VAR_STMT = @VAR_STMT + '    ) ';
	   SET @VAR_STMT = @VAR_STMT + '  END ';
	   SET @VAR_STMT = @VAR_STMT + CHAR(39) + '); ';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 9 - Create Customer - Fact Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';

	   SET @VAR_STMT = @VAR_STMT + 'CREATE TABLE [FACT].[CUSTOMERS] ';
	   SET @VAR_STMT = @VAR_STMT + '( ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_id int not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_lname varchar(40), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_fname varchar(40), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_phone char(12), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_address varchar(40), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_city varchar(20), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_state char(2), ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_zip char(5) not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_package_key int not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_start_date_key int not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_end_date_key int not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_date_str varchar(10) not null, ';
	   SET @VAR_STMT = @VAR_STMT + '  cus_qtr_key as [DIM].[UFN_GET_DATE_HASH] (cus_date_str)  ';
	   SET @VAR_STMT = @VAR_STMT + '); ';

	   SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS] ADD CONSTRAINT PK_CUSTOMERS_ID PRIMARY KEY CLUSTERED (cus_id); ';
       SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS] ADD CONSTRAINT DF_START_DTE DEFAULT (1) FOR [cus_start_date_key]; ';
       SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS]  ADD CONSTRAINT DF_END_DTE DEFAULT (365) FOR [cus_end_date_key]; ';

       SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS] ';
       SET @VAR_STMT = @VAR_STMT + 'ADD CONSTRAINT FK_DIM_PIG_PACKAGES FOREIGN KEY (cus_package_key) ';
       SET @VAR_STMT = @VAR_STMT + '    REFERENCES [DIM].[PIG_PACKAGES] (pkg_id); ';
	   
       SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS] ';
       SET @VAR_STMT = @VAR_STMT + 'ADD CONSTRAINT FK_DIM_START_DATE FOREIGN KEY (cus_start_date_key) ';
       SET @VAR_STMT = @VAR_STMT + '    REFERENCES [DIM].[DIM_DATE] (date_key); ';

       SET @VAR_STMT = @VAR_STMT + 'ALTER TABLE [FACT].[CUSTOMERS] ';
       SET @VAR_STMT = @VAR_STMT + 'ADD CONSTRAINT FK_DIM_END_DATE FOREIGN KEY (cus_end_date_key) ';
       SET @VAR_STMT = @VAR_STMT + '    REFERENCES [DIM].[DIM_DATE] (date_key); ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 10 - Load Pig Packages - Dimension Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'INSERT INTO [DIM].[PIG_PACKAGES] ([pkg_name]) SELECT ([pkg_name]) FROM [BIG_JONS_BBQ_DW].[DIM].[PIG_PACKAGES] WITH (NOLOCK); ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 11 - Load Date - Dimension Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'INSERT INTO [DIM].[DIM_DATE] ';
	   SET @VAR_STMT = @VAR_STMT + '( ';
	   SET @VAR_STMT = @VAR_STMT + '      [date_date] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_string] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_month] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_day] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_qtr] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_doy] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_yyyy] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_mm] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_dd] ';
	   SET @VAR_STMT = @VAR_STMT + ') ';

	   SET @VAR_STMT = @VAR_STMT + 'SELECT '; 
	   SET @VAR_STMT = @VAR_STMT + '      [date_date] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_string] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_month] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_day] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_qtr] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_doy] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_yyyy] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_mm] ';
       SET @VAR_STMT = @VAR_STMT + '     ,[date_int_dd] ';
	   SET @VAR_STMT = @VAR_STMT + 'FROM [BIG_JONS_BBQ_DW].[DIM].[DIM_DATE] WITH (NOLOCK); ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


       --
       -- 12 - Load Customer - Fact Table
       --

	   SET @VAR_STMT = '';
       SET @VAR_STMT = @VAR_STMT + 'USE [' + @VAR_NAME + ']; ';
	   SET @VAR_STMT = @VAR_STMT + 'INSERT INTO [FACT].[CUSTOMERS]  ';
	   SET @VAR_STMT = @VAR_STMT + '( ';
	   SET @VAR_STMT = @VAR_STMT + '      [cus_id] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_lname] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_fname] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_phone] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_address] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_city] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_state] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_zip] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_package_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_start_date_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_end_date_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_date_str] ';
	   SET @VAR_STMT = @VAR_STMT + ') ';

	   SET @VAR_STMT = @VAR_STMT + 'SELECT ';
	   SET @VAR_STMT = @VAR_STMT + '      [cus_id] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_lname] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_fname] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_phone] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_address] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_city] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_state] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_zip] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_package_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_start_date_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_end_date_key] ';
	   SET @VAR_STMT = @VAR_STMT + '     ,[cus_date_str] ';
	   SET @VAR_STMT = @VAR_STMT + 'FROM [BIG_JONS_BBQ_DW].[FACT].[CUSTOMERS] WITH (NOLOCK)  ';
	   SET @VAR_STMT = @VAR_STMT + 'WHERE [cus_qtr_key] = ' + CHAR(39) + @VAR_TAG + CHAR(39) + '; ';

	   -- Execute the TSQL
	   EXECUTE (@VAR_STMT);

	   -- Debugging
	   --PRINT @VAR_STMT;


	   -- Increment the quarter
	   SET @VAR_QTR = @VAR_QTR + 1

	END

	-- Increment the year
	SET @VAR_YEAR = @VAR_YEAR + 1
END
GO