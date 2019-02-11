/******************************************************
 *
 * Name:         big-johns-bbq-table-partitioning.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Convert the Big Jons BBQ data warehouse
 *               into a table partitioning example.
 *
 *               Implement a sliding window with hospital
 *               partitions.
 *
 ******************************************************/

 
--
-- 1 - Create primary database to house Dimension tables
--

-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'BBQ_TABLE_PART')
    DROP DATABASE [BBQ_TABLE_PART]
GO

-- Add new databases
CREATE DATABASE [BBQ_TABLE_PART] ON
PRIMARY
    ( NAME = N'BBQ_TABLE_PART_DAT', FILENAME = N'C:\MSSQL\DATA\BBQ_TABLE_PART_DW.MDF' ,
    SIZE = 4MB , MAXSIZE = UNLIMITED, FILEGROWTH = 4MB)
LOG ON
    ( NAME = N'BBQ_TABLE_PART_LOG', FILENAME = N'C:\MSSQL\LOG\BBQ_TABLE_PART_DW.LDF' ,
    SIZE = 4MB , MAXSIZE = UNLIMITED, FILEGROWTH = 4MB );
GO 

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::BBQ_TABLE_PART TO SA;
GO

-- Show the new database
SELECT * FROM sys.databases WHERE name = 'BBQ_TABLE_PART';
GO


--
-- 2 - Create Fact schema
--

-- Which database to use.
USE [BBQ_TABLE_PART]
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
USE [BBQ_TABLE_PART]
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
USE [BBQ_TABLE_PART];
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
)
ON [PRIMARY]
WITH (DATA_COMPRESSION = PAGE)
;
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
USE [BBQ_TABLE_PART];
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
USE [BBQ_TABLE_PART];
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
USE [BBQ_TABLE_PART];
GO

-- Add the data to the table
INSERT INTO [DIM].[PIG_PACKAGES] ([pkg_name]) 
  SELECT ([pkg_name]) FROM [BIG_JONS_BBQ_DW].[DIM].[PIG_PACKAGES] WITH (NOLOCK);
  


--
-- 8 - Load Date - Dimension Table
--

-- Use the correct database
USE [BBQ_TABLE_PART];
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
-- 9 - Make two helper functions (kudos to Kalen Delaney for base code)
-- 

-- Get the name of an index by ordinal position
EXEC('CREATE FUNCTION [DBO].[UFN_GET_INDEX_NAME] (@object_id int, @index_id tinyint)
RETURNS sysname
AS
BEGIN
    RETURN
	(
    SELECT name 
    FROM sys.indexes
    WHERE object_id = @object_id and index_id = @index_id
    )
END;');
GO


-- Return all partition information for the database
CREATE VIEW [DBO].[UFN_GET_PARTITION_INFO] AS
    SELECT 
        OBJECT_NAME(i.[object_id]) AS ObjectName, 
        (SELECT [dbo].[UFN_GET_INDEX_NAME] (i.[object_id], i.index_id)) AS IndexName, 
	    p.[partition_number] AS PartitionNo, 
	    fg.[name] AS FileGroupName, 
	    p.[rows] AS TotalRows, 
	    au.[total_pages] AS TotalPages,
	    CASE pf.[boundary_value_on_right] 
            WHEN 1 THEN 'less than' 
            ELSE 'less than or equal to' 
        END AS CompareType, 
        rv.[value] AS CompareValue

    FROM 
        sys.partitions p JOIN sys.indexes i
	        ON p.[object_id] = i.[object_id] AND 
			   p.[index_id] = i.[index_id]
        JOIN sys.partition_schemes ps 
            ON ps.[data_space_id] = i.[data_space_id]
        JOIN sys.partition_functions pf 
		    ON pf.[function_id] = ps.[function_id]
        LEFT JOIN sys.partition_range_values rv    
            ON pf.[function_id] = rv.[function_id] AND 
			   p.[partition_number] = rv.[boundary_id]
        JOIN sys.destination_data_spaces dds
		    ON dds.[partition_scheme_id] = ps.[data_space_id] AND 
			   dds.[destination_id] = p.[partition_number]
 	    JOIN sys.filegroups fg 
		    ON dds.[data_space_id] = fg.[data_space_id]
	    JOIN 
	      (
	         SELECT 
			     [container_id], 
				 sum([total_pages]) as total_pages 
		     FROM 
			     sys.allocation_units
		     GROUP BY 
			     [container_id]
		  ) AS au
		    ON au.[container_id] = p.[partition_id]
    WHERE 
	    i.[index_id] < 2;
GO



--
-- 10 - Make two hospital partitions
-- 

-- Before invalid values
alter database [BBQ_TABLE_PART] add filegroup FG00HOSPITAL
go

alter database [BBQ_TABLE_PART] add file 
(
    NAME=FN00HOSPITAL,    
    FILENAME='C:\MSSQL\DATA\FN00HOSPITAL.NDF',
    SIZE=4MB, 
    FILEGROWTH=4MB)
TO FILEGROUP FG00HOSPITAL
GO


-- After invalid values
alter database [BBQ_TABLE_PART] add filegroup FG99HOSPITAL
go

alter database [BBQ_TABLE_PART] add file 
(
    NAME=FN99HOSPITAL,    
    FILENAME='C:\MSSQL\DATA\FN99HOSPITAL.NDF',
    SIZE=4MB, 
    FILEGROWTH=4MB)
TO FILEGROUP FG99HOSPITAL
GO



--
-- 11 - Make eight files/file groups, one partition function, one partition scheme
-- 

-- Declare local variables
DECLARE @VAR_YEAR1 INT = 2011;
DECLARE @VAR_QTR1 INT = 1;
DECLARE @VAR_TAG1 CHAR(7) = '2011001';
DECLARE @VAR_NAME1 VARCHAR(64);

DECLARE @VAR_STMT1 VARCHAR(MAX);
DECLARE @VAR_STMT2 VARCHAR(MAX);
DECLARE @VAR_STMT3 VARCHAR(MAX);

-- Work space
SET @VAR_STMT1 = '';

-- Partition Function 
SELECT @VAR_STMT2 = 'CREATE PARTITION FUNCTION PF_HASH_BY_QTR (INT) AS RANGE LEFT FOR VALUES (2010004,';

-- Partition Scheme
SELECT @VAR_STMT3 = 'CREATE PARTITION SCHEME PS_HASH_BY_QTR AS PARTITION PF_HASH_BY_QTR TO ';
SELECT @VAR_STMT3 = @VAR_STMT3 + '(FG00HOSPITAL,';


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
	   SET @VAR_NAME1 = 'TABLE_PART_' + @VAR_TAG1;


       -- A - Add file group    
       SELECT @VAR_STMT1 = 'ALTER DATABASE [BBQ_TABLE_PART] ADD FILEGROUP [FG_' +  @VAR_NAME1 + '];';

       -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;
    

       -- B - Add the file  
       SELECT @VAR_STMT1 = 'ALTER DATABASE [BBQ_TABLE_PART] ADD FILE (NAME=FN_' + @VAR_NAME1 + 
                          ', FILENAME=''C:\MSSQL\DATA\FN_' +  @VAR_NAME1 + 
                          '.NDF'', SIZE=8MB, FILEGROWTH=2MB) TO FILEGROUP [FG_' +  @VAR_NAME1 + '];';
						          
       -- Execute the TSQL
	   EXECUTE (@VAR_STMT1);
	   PRINT @VAR_STMT1;


       -- C - Make up partition function
       SELECT @VAR_STMT2 = @VAR_STMT2 + @VAR_TAG1 + ',';


       -- D - Make up partition scheme
       SELECT @VAR_STMT3 = @VAR_STMT3 + 'FG_' +  @VAR_NAME1 + ',';


	   -- Increment the quarter
	   SET @VAR_QTR1 = @VAR_QTR1 + 1

	END

	-- Increment the year
	SET @VAR_YEAR1 = @VAR_YEAR1 + 1
END


-- Remove last comma
SELECT @VAR_STMT2 = SUBSTRING(@VAR_STMT2, 1, LEN(@VAR_STMT2) -1) + ');';

-- Execute the TSQL
EXECUTE (@VAR_STMT2);
PRINT @VAR_STMT2;


-- Add the last file group (partition)
SELECT @VAR_STMT3 = @VAR_STMT3 + 'FG99HOSPITAL)';

-- Execute the TSQL
EXECUTE (@VAR_STMT3);
PRINT @VAR_STMT3;

GO


--
-- 12 - Create table using partition scheme & page compression
-- 

CREATE TABLE [FACT].[CUSTOMERS]
(
	[cus_id] [int] NOT NULL,
	[cus_lname] [varchar](40) NULL,
	[cus_fname] [varchar](40) NULL,
	[cus_phone] [char](12) NULL,
	[cus_address] [varchar](40) NULL,
	[cus_city] [varchar](20) NULL,
	[cus_state] [char](2) NULL,
	[cus_zip] [char](5) NOT NULL,
	[cus_package_key] [int] NOT NULL,
	[cus_start_date_key] [int] NOT NULL,
	[cus_end_date_key] [int] NOT NULL,
	[cus_date_str] [varchar](10) NOT NULL,
	[cus_qtr_key] int not null,

    CONSTRAINT [PK_CUSTOMERS_ID] PRIMARY KEY CLUSTERED 
    ([cus_id], [cus_qtr_key])
) 
ON PS_HASH_BY_QTR ([cus_qtr_key])
WITH (DATA_COMPRESSION = PAGE)
GO



--
-- 13 - Load the partitioned table with data
--

INSERT INTO [BBQ_TABLE_PART].[FACT].[CUSTOMERS]
SELECT *
FROM [BIG_JONS_BBQ_DW].[FACT].[CUSTOMERS] WITH (NOLOCK);
GO



--
-- 14 - Create partition hash key table for sliding window
--

-- Use the correct database
USE [BBQ_TABLE_PART];
GO

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DIM].[PARTITIONS]') AND
    type in (N'U'))
DROP TABLE [DIM].[PARTITIONS]
GO

-- Create new table
CREATE TABLE [DIM].[PARTITIONS]
(
   part_id int not null identity (1, 1),
   part_hash_key int not null
);
GO

-- Alter the table (primary key)
ALTER TABLE [DIM].[PARTITIONS]
   ADD CONSTRAINT PK_PARTITIONS_ID PRIMARY KEY CLUSTERED (part_id);
GO

-- Alter the table (primary key)
ALTER TABLE [DIM].[PARTITIONS]
   ADD CONSTRAINT UNQ_PARTITIONS_HASH UNIQUE (part_hash_key);
GO

-- Add data to the table
INSERT INTO [DIM].[PARTITIONS] (part_hash_key)
VALUES
    (2010001),
    (2010002),
    (2010003),
    (2010004),
    (2011001),
    (2011002),
    (2011003),
    (2011004),
    (2012001),
    (2012002),
    (2012003),
    (2012004),
    (2013001),
    (2013002),
    (2013003),
    (2013004),
    (2014001),
    (2014002),
    (2014003),
    (2014004),
    (2015001),
    (2015002),
    (2015003),
    (2015004);



--
-- 15 - Create a function to add a new partition
--

-- Delete existing procedure
IF OBJECT_ID(N'[dbo].[USP_ADD_CUSTOMER_PARTITION]') > 0
    DROP PROCEDURE [dbo].[USP_ADD_CUSTOMER_PARTITION]
GO

-- Create new procedure
CREATE PROCEDURE [dbo].[USP_ADD_CUSTOMER_PARTITION]
AS

    -- No info messages
    SET NOCOUNT ON;

    -- Declare variables
    DECLARE @VAR_VAL1 INT;
    DECLARE @VAR_STR1 VARCHAR(128);
    DECLARE @VAR_SQL1 VARCHAR(MAX);

    -- Get the next max value
    SELECT 
        @VAR_VAL1 = [part_id],
        @VAR_STR1 = [part_hash_key]
    FROM 
        [DIM].[PARTITIONS] AS P1
    WHERE
	    P1.part_id = 
	(		 
        SELECT 
            P2.[part_id] + 1
        FROM 
            [DIM].[PARTITIONS] AS P2
        WHERE
	        P2.[part_hash_key] =
	    (
            SELECT CONVERT(INT, MAX(P3.CompareValue)) 
            FROM [dbo].[UFN_GET_PARTITION_INFO] as P3
	    )
    );      

    -- Add new file group
    SET @VAR_SQL1 = 'ALTER DATABASE [BBQ_TABLE_PART] ADD FILEGROUP FG_TABLE_PART_' + @VAR_STR1 + ';';
    PRINT @VAR_SQL1;
    EXEC(@VAR_SQL1);
          
    -- Add new file 
    SET @VAR_SQL1 = '';
    SET @VAR_SQL1 = @VAR_SQL1 + 'ALTER DATABASE [BBQ_TABLE_PART] ADD FILE ';
    SET @VAR_SQL1 = @VAR_SQL1 + '( ';
    SET @VAR_SQL1 = @VAR_SQL1 + '    NAME=FN_TABLE_PART_' + @VAR_STR1 + ',';
    SET @VAR_SQL1 = @VAR_SQL1 + '    FILENAME=' + CHAR(39) + 'C:\MSSQL\DATA\FN_TABLE_PART_' + @VAR_STR1 + '.NDF' + CHAR(39) + ',';
    SET @VAR_SQL1 = @VAR_SQL1 + '    SIZE=10MB, ';
    SET @VAR_SQL1 = @VAR_SQL1 + '    FILEGROWTH=2MB ';
    SET @VAR_SQL1 = @VAR_SQL1 + ') ';
    SET @VAR_SQL1 = @VAR_SQL1 + 'TO FILEGROUP FG_TABLE_PART_' + @VAR_STR1 + ';';
    PRINT @VAR_SQL1;
    EXEC(@VAR_SQL1);

    -- Set as new place to store data
    SET @VAR_SQL1 = '';
    SET @VAR_SQL1 = @VAR_SQL1 + 'ALTER PARTITION SCHEME PS_HASH_BY_QTR ';    
    SET @VAR_SQL1 = @VAR_SQL1 + 'NEXT USED FG_TABLE_PART_' + + @VAR_STR1 + ';';    
    PRINT @VAR_SQL1;
    EXEC(@VAR_SQL1);
  
    -- Add at new partition at max value
    SET @VAR_SQL1 = '';
    SET @VAR_SQL1 = @VAR_SQL1 + 'ALTER PARTITION FUNCTION PF_HASH_BY_QTR () ';    
    SET @VAR_SQL1 = @VAR_SQL1 + 'SPLIT RANGE (' + LTRIM(RTRIM(@VAR_STR1)) + ');';    
    PRINT @VAR_SQL1;
    EXEC(@VAR_SQL1);

GO



--
-- 16 - Create a function to add a new table
--

-- Delete existing procedure
IF OBJECT_ID(N'[dbo].[USP_ADD_CUSTOMER_TEMP_TBL]') > 0
    DROP PROCEDURE [dbo].[USP_ADD_CUSTOMER_TEMP_TBL]
GO

-- Create new procedure
CREATE PROCEDURE [dbo].[USP_ADD_CUSTOMER_TEMP_TBL]
    @FG_NAME VARCHAR(250)
AS

    -- No info messages
    SET NOCOUNT ON;

    -- Declare variables
    DECLARE @VAR_SQL2 VARCHAR(MAX);

    -- Create temp table on oldest partition
    SET @VAR_SQL2 = '';
    SET @VAR_SQL2 = @VAR_SQL2 + ' CREATE TABLE [FACT].[TMP_CUSTOMERS] ';
    SET @VAR_SQL2 = @VAR_SQL2 + '( ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_id] [int] NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_lname] [varchar](40) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_fname] [varchar](40) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_phone] [char](12) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_address] [varchar](40) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_city] [varchar](20) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_state] [char](2) NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_zip] [char](5) NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_package_key] [int] NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_start_date_key] [int] NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_end_date_key] [int] NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_date_str] [varchar](10) NOT NULL, ';
	SET @VAR_SQL2 = @VAR_SQL2 + '    [cus_qtr_key] int not null, ';

    SET @VAR_SQL2 = @VAR_SQL2 + '    CONSTRAINT [PK_CUSTOMERS_TEMP_ID] PRIMARY KEY CLUSTERED  ';
    SET @VAR_SQL2 = @VAR_SQL2 + '    ([cus_id], [cus_qtr_key]) ';
    SET @VAR_SQL2 = @VAR_SQL2 + ') ';

    SET @VAR_SQL2 = @VAR_SQL2 + '	ON ['+ @FG_NAME + ']';
    SET @VAR_SQL2 = @VAR_SQL2 + '	WITH (DATA_COMPRESSION = PAGE);';
  
    PRINT @VAR_SQL2;
    EXEC(@VAR_SQL2);

GO


--
-- 17 - Create a function to drop a old partition
--

-- Delete existing procedure
IF OBJECT_ID(N'[dbo].[USP_DEL_CUSTOMER_PARTITION]') > 0
    DROP PROCEDURE [dbo].[USP_DEL_CUSTOMER_PARTITION]
GO

-- Create new procedure
CREATE PROCEDURE [dbo].[USP_DEL_CUSTOMER_PARTITION]
AS
    -- No info messages
    SET NOCOUNT ON;

    -- Declare variables
    DECLARE @VAR_VAL3 INT;
    DECLARE @VAR_STR3 VARCHAR(128);  
    DECLARE @VAR_VAL4 INT;
    DECLARE @VAR_STR4 VARCHAR(128);    
    DECLARE @VAR_SQL5 VARCHAR(MAX);

    -- Get information for partition 1
    SELECT 
         @VAR_VAL3 = [part_id],
         @VAR_STR3 = [part_hash_key]
    FROM 
        [DIM].[PARTITIONS] AS P1
    WHERE
	    P1.part_id = 
	(		 
        SELECT 
            P2.[part_id] + 0
        FROM 
            [DIM].[PARTITIONS] AS P2
        WHERE
	        P2.[part_hash_key] =
	    (
            SELECT CONVERT(INT, MIN(P3.CompareValue)) 
            FROM [dbo].[UFN_GET_PARTITION_INFO] as P3
	    )
    );  
		  
    -- Get information for partition 2
    SELECT 
         @VAR_VAL4 = [part_id],
         @VAR_STR4 = [part_hash_key]
    FROM 
        [DIM].[PARTITIONS] AS P1
    WHERE
	    P1.part_id = 
	(		 
        SELECT 
            P2.[part_id] + 1
        FROM 
            [DIM].[PARTITIONS] AS P2
        WHERE
	        P2.[part_hash_key] =
	    (
            SELECT CONVERT(INT, MIN(P3.CompareValue)) 
            FROM [dbo].[UFN_GET_PARTITION_INFO] as P3
	    )
    );  

  -- Add new temporary table on partition 2
  SET @VAR_SQL5 = 'EXEC [dbo].[USP_ADD_CUSTOMER_TEMP_TBL] ' + CHAR(39) + 'FG_TABLE_PART_' + @VAR_STR4 + CHAR(39);
  PRINT @VAR_SQL5;
  EXEC(@VAR_SQL5);
          
  -- Move data from part 2 to temporary table
  SET @VAR_SQL5 = 'ALTER TABLE [FACT].[CUSTOMERS] SWITCH PARTITION 2 TO [FACT].[TMP_CUSTOMERS]';
  PRINT @VAR_SQL5;
  EXEC(@VAR_SQL5);

  -- Remove from function - part 1
  SET @VAR_SQL5 = 'ALTER PARTITION FUNCTION PF_HASH_BY_QTR () MERGE RANGE (' + STR(@VAR_STR3) + ');';
  PRINT @VAR_SQL5;
  EXEC(@VAR_SQL5);

  -- Remove from function - part 2
  SET @VAR_SQL5 = 'ALTER PARTITION FUNCTION PF_HASH_BY_QTR () MERGE RANGE (' + STR(@VAR_STR4) + ');';
  PRINT @VAR_SQL5;
  EXEC(@VAR_SQL5);

  -- drop the temporary table
  SET @VAR_SQL5 = 'DROP TABLE [FACT].[TMP_CUSTOMERS];'
  PRINT @VAR_SQL5;  
  EXEC(@VAR_SQL5);

  -- drop the file
  SET @VAR_SQL5 = 'ALTER DATABASE [BBQ_TABLE_PART] REMOVE FILE [FN_TABLE_PART_' + @VAR_STR4 + '];';
  PRINT @VAR_SQL5;  
  EXEC(@VAR_SQL5);

  -- drop the filegroup
  SET @VAR_SQL5 = 'ALTER DATABASE [BBQ_TABLE_PART] REMOVE FILEGROUP [FG_TABLE_PART_' + @VAR_STR4 + '];';
  PRINT @VAR_SQL5;  
  EXEC(@VAR_SQL5);

  -- Set hospital before as new place to store data
  SET @VAR_SQL5 = 'ALTER PARTITION SCHEME PS_HASH_BY_QTR NEXT USED [FG00HOSPITAL];';
  PRINT @VAR_SQL5;  
  EXEC(@VAR_SQL5);

  -- Split hospital on old value
  SET @VAR_SQL5 = 'ALTER PARTITION FUNCTION PF_HASH_BY_QTR () SPLIT RANGE (' + @VAR_STR4 + ');';
  PRINT @VAR_SQL5;  
  EXEC(@VAR_SQL5);
    
GO


