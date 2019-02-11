/******************************************************
 *
 * Name:         asd-demo-script.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-25-2015
 *     Purpose:  A program to create a fictuous
 *               banking operation.
 *     Note:     Script was tested in both Azure
 *               SQL database V12 and SQL Server 2014.
 * 
 ******************************************************/


/*  
	1 - Create prime number database
*/

-- https://msdn.microsoft.com/en-us/library/dn268335.aspx

-- Which database to use.
USE [master]
GO

-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'JDBANK')
DROP DATABASE JDBANK
GO


-- Create new database - azure sql database - ssas
CREATE DATABASE [JDBANK] 
(
MAXSIZE = 20GB,
EDITION = 'STANDARD',
SERVICE_OBJECTIVE = 'S1'
)
GO   


-- Define the db with default groups - on premise
/*
CREATE DATABASE [JDBANK]
ON PRIMARY
( NAME = 'JdBankData',
    FILENAME = 'C:\MSSQL\DATA\JDBank_Data.mdf',
    SIZE = 64MB,
    MAXSIZE = 128MB,
    FILEGROWTH = 32MB)
LOG ON
 ( NAME = 'JdBankLog',
    FILENAME = 'C:\MSSQL\LOG\JDBank_Log.ldf',
    SIZE = 32MB,
    MAXSIZE = 64MB,
    FILEGROWTH = 32MB)
GO
*/

-- Show the new database
SELECT * FROM sys.databases;
GO



/*  
	2 - Create server login & master user
*/

-- https://msdn.microsoft.com/en-us/library/ee336235.aspx


-- Which database to use.
USE [master]
GO


-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.sql_logins WHERE name = N'BANK_USER')
DROP LOGIN [BANK_USER]
GO

-- Add new login to master
CREATE LOGIN [BANK_USER] WITH PASSWORD=N'M0a3r2c0h15$';
GO

-- Show the logins 
SELECT * FROM sys.sql_logins
GO


-- Delete existing user.
IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BANK_USER')
DROP USER [BANK_USER]
GO

-- Add new user to master
CREATE USER [BANK_USER] FROM LOGIN [BANK_USER];
GO

-- Show the users
SELECT * FROM sys.database_principals
GO



/*
   3 - Create Daily schema
*/

-- https://msdn.microsoft.com/en-us/library/ms189462.aspx

-- Which database to use.
USE [JDBANK]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'DAILY')
DROP SCHEMA [DAILY]
GO

-- Add new schema.
CREATE SCHEMA [DAILY] AUTHORIZATION [dbo]
GO



/*
   4 - Create Monthly schema
*/

-- Which database to use.
USE [JDBANK]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'MONTHLY')
DROP SCHEMA [MONTHLY]
GO

-- Add new schema.
CREATE SCHEMA [MONTHLY] AUTHORIZATION [dbo]
GO



/*
   5 - Show newly created schemas
*/

-- Which database to use.
USE [JDBANK]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name IN ('DAILY', 'MONTHLY');
GO



/*
   6 - Create Database user
*/

-- https://msdn.microsoft.com/en-us/library/ms189751.aspx

-- Which database to use.
USE [JDBANK]
GO

-- Delete existing user.
IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BANK_USER')
DROP USER [BANK_USER]
GO

-- Add new user.
CREATE USER [BANK_USER] FOR LOGIN [BANK_USER] WITH DEFAULT_SCHEMA=[DAILY]
GO

-- Show the new database
SELECT * FROM sys.database_principals;
GO



/*
   7 - Assign rights at schema level
*/

-- Use the correct database
USE [JDBANK];
GO

-- Select access to tables
GRANT SELECT ON SCHEMA::DAILY TO BANK_USER;
GO

-- Definition access to procedures
GRANT VIEW DEFINITION ON SCHEMA::DAILY TO BANK_USER;
GO



/*
    8 - Create the [customer] table
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[CUSTOMERS]') AND
    type in (N'U'))
DROP TABLE [DAILY].[CUSTOMERS]
GO

-- Create new table
CREATE TABLE [DAILY].[CUSTOMERS]
(
	[cus_id] [int] NOT NULL,
	[cus_lname] [varchar](40) NULL,
	[cus_fname] [varchar](40) NULL,
	[cus_phone] [char](12) NULL,
	[cus_address] [varchar](40) NULL,
	[cus_city] [varchar](20) NULL,
	[cus_state] [char](2) NULL,
	[cus_zip] [char](5) NOT NULL,
	[cus_date_str] [varchar](10) NOT NULL
)
GO

-- Primary key constraint
ALTER TABLE [DAILY].[CUSTOMERS] ADD CONSTRAINT PK_CUS_ID
PRIMARY KEY CLUSTERED (cus_id);
GO



/*
    9 - Create the [account] table
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[ACCOUNTS]') AND
    type in (N'U'))
DROP TABLE [DAILY].[ACCOUNTS]
GO

-- Create new table
CREATE TABLE [DAILY].[ACCOUNTS]
(
	[cus_id] [int] NOT NULL,
	[acc_routing_no] [char] (9) NULL,
	[acc_transit_no] [char] (10) NULL,
	[acc_type] [char] NOT NULL,
	[acc_id] INT NOT NULL IDENTITY(1, 1)
)
GO

-- Check constraint
ALTER TABLE [DAILY].[ACCOUNTS] ADD CONSTRAINT CK_ACC_TYPE
    CHECK (ACC_TYPE IN ('S', 'C'));

-- Default constraint
ALTER TABLE [DAILY].[ACCOUNTS] ADD CONSTRAINT DF_ACC_TYPE
    DEFAULT 'C' FOR ACC_TYPE;

-- Primary key constraint
ALTER TABLE [DAILY].[ACCOUNTS] ADD CONSTRAINT PK_ACC_ID
PRIMARY KEY CLUSTERED (acc_id);

-- Foreign key constraint
ALTER TABLE [DAILY].[ACCOUNTS]
ADD CONSTRAINT FK_CUSTOMER_ID FOREIGN KEY (cus_id)
    REFERENCES [DAILY].[CUSTOMERS] (cus_id) 
	ON DELETE CASCADE
	ON UPDATE CASCADE;
GO



/*
    10 - Create the [transaction] table
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing table
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[TRANSACTIONS]') AND
    type in (N'U'))
DROP TABLE [DAILY].[TRANSACTIONS]
GO

-- Create new table
CREATE TABLE [DAILY].[TRANSACTIONS]
(
	[acc_id] [int] NOT NULL,
	[trn_date] datetime,
	[trn_amount] money,
	[trn_id] INT NULL
)
GO

-- Primary key constraint
ALTER TABLE [DAILY].[TRANSACTIONS] ADD CONSTRAINT UQ_ACC_ID
UNIQUE CLUSTERED (trn_id);

-- Foreign key constraint
ALTER TABLE [DAILY].[TRANSACTIONS]
ADD CONSTRAINT FK_ACCT_ID FOREIGN KEY (acc_id)
    REFERENCES [DAILY].[ACCOUNTS] (acc_id) 
	ON DELETE CASCADE
	ON UPDATE CASCADE;
GO



/*
    11 - Create a sequence for the trans id
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing sequence
IF  EXISTS (
    SELECT * FROM sys.sequences
    WHERE object_id = OBJECT_ID(N'[DAILY].[SEQ_TRANS]'))
DROP SEQUENCE [DAILY].[SEQ_TRANS]
GO

-- Create a new sequence
CREATE SEQUENCE [DAILY].[SEQ_TRANS]
    AS INT 
    START WITH 1
    INCREMENT BY 2
    CYCLE;
GO

-- Show the sequence
SELECT * FROM sys.sequences;
GO

-- Add the default constraint
ALTER TABLE [DAILY].[TRANSACTIONS] ADD CONSTRAINT DF_TRAN_ID
    DEFAULT (NEXT VALUE FOR [DAILY].[SEQ_TRANS]) FOR TRN_ID;
GO



/*
    12 - Add data to [customers] table via bcp batch file
*/

-- Use the correct database
USE [JDBANK];
GO

--  Run c:\asd\import-customers-bcp.bat for cloud database

--  Below code is used to import into on premise database

/*
BULK
INSERT [DAILY].[CUSTOMERS]
FROM 'C:\ASD\CUSTOMERS.TXT'
WITH
(
FIELDTERMINATOR = '\t',
ROWTERMINATOR = '\n'
);
*/


-- Show the data
SELECT * FROM [DAILY].[CUSTOMERS];



/*
    13 - Create a check digit function
*/

-- http://en.wikipedia.org/wiki/Luhn_algorithm

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing scalar valued function
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[GET_LUHN_ACCOUNT]') AND [type] in (N'FN')
	)
DROP FUNCTION [DAILY].[GET_LUHN_ACCOUNT]
GO

-- Create a new function
CREATE FUNCTION [DAILY].[GET_LUHN_ACCOUNT]
(
    @Luhn VARCHAR(7999)
)
RETURNS VARCHAR(8000)
AS
BEGIN
    IF @Luhn LIKE '%[^0-9]%'
        RETURN @Luhn

    DECLARE @Index SMALLINT,
        @Multiplier TINYINT,
        @Sum INT,
        @Plus TINYINT

    SELECT  @Index = LEN(@Luhn),
        @Multiplier = 2,
        @Sum = 0

    WHILE @Index >= 1
        SELECT  @Plus = @Multiplier * CAST(SUBSTRING(@Luhn, @Index, 1) AS TINYINT),
            @Multiplier = 3 - @Multiplier,
            @Sum = @Sum + @Plus / 10 + @Plus % 10,
            @Index = @Index - 1

    RETURN  @Luhn + CASE WHEN @Sum % 10 = 0 THEN '0' ELSE CAST(10 - @Sum % 10 AS CHAR) END
END
GO

-- Check digit is 6 at the end
SELECT [DAILY].[GET_LUHN_ACCOUNT]('750454597') AS ACCT_WITH_CHECK_DIGIT
GO



/*
    14 - Add random data to [accounts] table 
*/

-- Use the correct database
USE [JDBANK];
GO

-- Create accounts
INSERT INTO [DAILY].[ACCOUNTS]
SELECT 
    cus_id, 
	'129131673' AS routing_no,
	[DAILY].[GET_LUHN_ACCOUNT](right('000000000' + cast(abs(checksum(newid()))%1000000000 as varchar(9)), 9)) AS transit_no,
	CASE 
	    WHEN ABS(CHECKSUM(NEWID()) % 100) < 50 THEN 'S'
	    ELSE 'C'
	END AS acc_type
FROM DAILY.CUSTOMERS;
GO



/*
    15 - Stored procedure to create [transaciton] data
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing stored procedure
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[ADD_RAND_TRANSACTIONS]') AND [type] in (N'P')
	)
DROP PROCEDURE [DAILY].[ADD_RAND_TRANSACTIONS]
GO

-- Create new stored procedure
CREATE PROCEDURE [DAILY].[ADD_RAND_TRANSACTIONS]
    @ACC_ID INT = 1
AS
BEGIN

    -- Do not count
    SET NOCOUNT ON;

	-- How many records?
	DECLARE @CNT INT;
	DECLARE @MAX INT;

	-- Up to ten records
	SET @MAX = 1 + FLOOR(RAND() * 10);
	SET @CNT = 0;

	-- How many records
	PRINT 'Acc Id:' + STR(@ACC_ID, 4, 0) + SPACE(10) + 'Rec Cnt: ' + STR(@MAX, 4, 0)

	-- Add the data
	WHILE (@CNT < @MAX)
	BEGIN

	    -- Do not care about date order
	    INSERT INTO [DAILY].[TRANSACTIONS]
		(ACC_ID, TRN_DATE, TRN_AMOUNT)
        SELECT 
            @ACC_ID,
	        dateadd(d, checksum(newid()) % 100, getdate()) as trn_date,
	        checksum(newid()) % 1000 as trn_amount

		-- Increment counter
		SET @CNT = @CNT + 1;
	END
END
GO



/*
    16 - Cursor to add x records per account
*/

-- Use the correct database
USE [JDBANK];
GO

-- Declare local variables
DECLARE @MY_ID INT;

-- Allocate cursor, return table names
DECLARE MY_CURSE CURSOR FAST_FORWARD FOR 
    SELECT ACC_ID
    FROM DAILY.ACCOUNTS;

-- Open cursor    
OPEN MY_CURSE;

-- Get the first row    
FETCH NEXT FROM MY_CURSE INTO @MY_ID;

-- While there is data
WHILE (@@FETCH_STATUS = 0) 
BEGIN   

    -- Generate data
    EXEC [DAILY].[ADD_RAND_TRANSACTIONS] @ACC_ID = @MY_ID

	-- Get the next row    
	FETCH NEXT FROM MY_CURSE INTO @MY_ID;

END;

-- Close the cursor
CLOSE MY_CURSE; 

-- Release the cursor
DEALLOCATE MY_CURSE; 
GO



/*
    17 - Create a regular view of customer trans by month
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete existing view
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[MONTHLY].[TRANS_BY_CUSTOMER_MONTH]') AND [type] in (N'V')
	)
DROP VIEW [MONTHLY].[TRANS_BY_CUSTOMER_MONTH]
GO

-- Create new view
CREATE VIEW [MONTHLY].[TRANS_BY_CUSTOMER_MONTH]
AS
	WITH CTE_RAW_DATA
	AS
	(
	SELECT 
		UPPER(C.CUS_LNAME) AS LAST_NAME,
		UPPER(C.CUS_FNAME) AS FIRST_NAME,
		C.CUS_STATE AS US_STATE,
		A.ACC_TYPE AS ACCOUNT_TYPE,
		SUBSTRING(REPLACE(CONVERT(CHAR(10), TRN_DATE, 102), '.', ''), 1, 6) AS MONTH_KEY,
		TRN_AMOUNT	 
	FROM [DAILY].[CUSTOMERS] C JOIN [DAILY].[ACCOUNTS] A ON C.[cus_id] = A.[cus_id]
	JOIN [DAILY].[TRANSACTIONS] T ON A.[acc_id] = T.[acc_id]
	)
	SELECT 
		LAST_NAME,
		FIRST_NAME,
		US_STATE,
		ACCOUNT_TYPE,
		MONTH_KEY,
		SUM(TRN_AMOUNT) AS BALANCE 
	FROM CTE_RAW_DATA
	GROUP BY
		LAST_NAME,
		FIRST_NAME,
		US_STATE,
		MONTH_KEY,
		ACCOUNT_TYPE
GO

-- Show the ordered data - over clause
SELECT 
    ROW_NUMBER() OVER(PARTITION BY LAST_NAME, FIRST_NAME ORDER BY MONTH_KEY ASC) AS TRANS_ORD, 
    * 
FROM [MONTHLY].[TRANS_BY_CUSTOMER_MONTH]
ORDER BY
    LAST_NAME,
	FIRST_NAME,
	MONTH_KEY;
GO

-- Notice, Bank user can not see this schema!



/*
    18 - Create indexed view of customer balance
*/

-- Use the correct database
USE [JDBANK];
GO

-- Set required session options 
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET NUMERIC_ROUNDABORT OFF
SET QUOTED_IDENTIFIER ON
GO

-- Delete existing view
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[DAILY].[CUSTOMER_BALANCE]') AND [type] in (N'V')
	)
DROP VIEW [DAILY].[CUSTOMER_BALANCE]
GO

-- Create new view 
CREATE VIEW [DAILY].[CUSTOMER_BALANCE]
WITH SCHEMABINDING
AS
    SELECT 
		C.CUS_LNAME AS LAST_NAME,
		C.CUS_FNAME AS FIRST_NAME,
		C.CUS_STATE AS US_STATE,
		A.ACC_TYPE AS ACCOUNT_TYPE,
		SUM(ISNULL(TRN_AMOUNT, 0)) AS BALANCE,
		COUNT_BIG(*) AS RECORDS
	FROM [DAILY].[CUSTOMERS] C INNER JOIN [DAILY].[ACCOUNTS] A ON C.[cus_id] = A.[cus_id]
	INNER JOIN [DAILY].[TRANSACTIONS] T ON A.[acc_id] = T.[acc_id]
	GROUP BY
		C.CUS_LNAME,
		C.CUS_FNAME,
		C.CUS_STATE,
		A.ACC_TYPE
GO


/*
    View now uses space?
*/

-- Delete existing index
IF EXISTS (SELECT * FROM sysindexes WHERE name = 'IX_VIEW_CUSTOMER_BALANCE')
DROP INDEX IX_VIEW_CUSTOMER_BALANCE	ON [DAILY].[CUSTOMER_BALANCE]
GO

-- Space used by view before
sp_spaceused  '[DAILY].[CUSTOMER_BALANCE]'
GO
  
-- Covered index
CREATE UNIQUE CLUSTERED INDEX IX_VIEW_CUSTOMER_BALANCE 
    ON [DAILY].[CUSTOMER_BALANCE] (LAST_NAME, FIRST_NAME, US_STATE, ACCOUNT_TYPE);
GO

-- Space used by view after
sp_spaceused  '[DAILY].[CUSTOMER_BALANCE]'
GO


/*  ~ On premise verison only ~

-- Show pages allocated with thew view
DBCC IND('JDBANK', '[DAILY].[CUSTOMER_BALANCE]', 1)
GO

-- Show storage of data
DBCC TRACEON(3604)
DBCC PAGE('JDBANK',1,353,3) -- WITH TABLERESULTS
DBCC TRACEOFF(3604)
GO

*/


/*
    Does the query processor use the index?
*/

-- Update statistics
sp_updatestats
GO

-- Capture any query plans to query window
SET SHOWPLAN_XML ON;
GO

-- Use the index
SELECT 
  -- LAST_NAME, FIRST_NAME, US_STATE
  *
FROM [DAILY].[CUSTOMER_BALANCE]
WITH (NOEXPAND)
WHERE US_STATE IN ('MA', 'CT', 'RI', 'NH', 'VT', 'ME')
GO

-- Do not capture plans
SET SHOWPLAN_XML OFF;
GO


/*
	Various windowing functions
*/

-- Show various windowing functions
SELECT
    RANK() OVER(ORDER BY BALANCE) AS RANK_ORDER, 
    DENSE_RANK() OVER(ORDER BY BALANCE) AS DENSE_ORDER,
	NTILE(4) OVER(ORDER BY BALANCE) AS QUARTILE, 
    * 
FROM [DAILY].[CUSTOMER_BALANCE]
WITH (NOEXPAND)
GO



/*
    19 - Create patriot act trigger (10K)
*/

-- Use the correct database
USE [JDBANK];
GO

-- Delete the existing trigger.
IF EXISTS (select * from sysobjects where id = object_id('TRG_PATRIOT_ACT_VIOLATION') and type = 'TR')
   DROP TRIGGER [DAILY].[TRG_PATRIOT_ACT_VIOLATION]
GO

-- Create the new trigger.
CREATE TRIGGER [DAILY].[TRG_PATRIOT_ACT_VIOLATION] ON [DAILY].[TRANSACTIONS]
FOR INSERT, UPDATE, DELETE 
AS
BEGIN

    -- declare local variable
    DECLARE @MYMSG VARCHAR(250);

    -- nothing to do?
    IF (@@rowcount = 0) RETURN;

    -- do not count rows
    SET NOCOUNT ON;

    -- delete data
    IF (NOT EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)) 
    BEGIN
        PRINT 'No action required for delete record operation';
        RETURN;
    END;

    -- update data
    IF (EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)) 
    BEGIN
        PRINT 'No action required for update record operation';
        RETURN;
    END;

    -- delete data
    IF (NOT EXISTS (SELECT * FROM deleted) AND EXISTS (SELECT * FROM inserted)) 
    BEGIN
	    -- any large transactions?
		DECLARE @N INT = 0;
	    SELECT @N = COUNT([trn_amount]) FROM inserted WHERE ABS([trn_amount]) >= 10000;

	    -- any transactions > 10k
		IF (@N > 0)
		BEGIN
            PRINT 'Action required for insert record operation.  Patriot act violation detected.';
			ROLLBACK;
            RETURN;
		END;

		-- no big transactions
        PRINT 'No action required for update record operation';
        RETURN;
    END;
END
GO

-- Delete the last record
DECLARE @M INT = 0;
SELECT @M = MAX([trn_id]) FROM [DAILY].[TRANSACTIONS];
DELETE FROM [DAILY].[TRANSACTIONS] WHERE [trn_id] = @M;
GO

-- Update the second to last record
DECLARE @O INT = 0;
SELECT @O = MAX([trn_id]) FROM [DAILY].[TRANSACTIONS];
UPDATE [DAILY].[TRANSACTIONS] 
SET [trn_amount] = 500
WHERE [trn_id] = @O;
GO

-- Show the last transaction
SELECT TOP(1) * FROM [DAILY].[TRANSACTIONS] ORDER BY [trn_id] DESC;
GO

-- Insert a partiot act violation
INSERT INTO [DAILY].[TRANSACTIONS] (acc_id, trn_date, trn_amount)
VALUES (999, GETDATE(), 10001);
GO

-- Disable the trigger
DISABLE TRIGGER [DAILY].[TRG_PATRIOT_ACT_VIOLATION] ON [DAILY].[TRANSACTIONS];
GO



/*
    20 - Show objects that were created in demo
*/

select 
    o.name as obj_name,
    o.type as obj_type,
    o.type_desc 
from sys.objects o
where o.is_ms_shipped = 0
order by o.type
GO


-- Indexes are not in objects table
select o.name, o.type_desc, i.name, i.type_desc 
from sys.indexes i join sys.objects o
on i.object_id = o.object_id
where  o.is_ms_shipped = 0

