/******************************************************
 *
 * Name:         hybrid-data-platform-solutions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-07-2016
 *     Purpose:  A sample banking database.
 * 
 ******************************************************/



/*  
	1 - Create a database for banking information
*/


-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING01')
 DROP DATABASE [BANKING01]
GO


-- Add new databases.
CREATE DATABASE [BANKING01] ON  
 PRIMARY 
  ( NAME = N'BANKING_PRI_DAT', FILENAME = N'C:\MSSQL\DATA\BANKING.MDF' , SIZE = 32MB, FILEGROWTH = 16MB) 
 LOG ON 
  ( NAME = N'BANKING_ALL_LOG', FILENAME = N'C:\MSSQL\LOG\BANKING.LDF' , SIZE = 32MB, FILEGROWTH = 16MB)
GO


/*
   2 - Create Active schema
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ACTIVE')
DROP SCHEMA [ACTIVE]
GO

-- Add new schema.
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'ACTIVE';
GO



/*
   3 - Create history schema
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'HISTORY')
DROP SCHEMA [HISTORY]
GO

-- Add new schema.
CREATE SCHEMA [HISTORY] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'HISTORY';
GO



/*
   4 - Create server logins (admin/user)
*/

-- Which database to use.
USE [master]
GO


-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'BANK_ADMIN')
DROP LOGIN [BANK_ADMIN]
GO

-- Add new login.
CREATE LOGIN [BANK_ADMIN] WITH PASSWORD=N'M0a2r0c9h16$', DEFAULT_DATABASE=[BANKING01]
GO


-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'BANK_USER')
DROP LOGIN [BANK_USER]
GO

-- Add new login.
CREATE LOGIN [BANK_USER] WITH PASSWORD=N'A0p4r0i7l16#', DEFAULT_DATABASE=[BANKING01]
GO



/*
   5 - Create database user (admin/user)
*/
  
-- Which database to use.
USE [BANKING01]
GO

-- Delete existing user.
IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BANK_ADMIN')
DROP USER [BANK_ADMIN]
GO

-- Add new user.
CREATE USER [BANK_ADMIN] FOR LOGIN [BANK_ADMIN] WITH DEFAULT_SCHEMA=[ACTIVE]
GO


-- Delete existing user.
IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'BANK_USER')
DROP USER [BANK_USER]
GO

-- Add new user.
CREATE USER [BANK_USER] FOR LOGIN [BANK_USER] WITH DEFAULT_SCHEMA=[ACTIVE]
GO



/*
   6 - Grant rights at schema level
*/

-- Tables and views
GRANT SELECT ON SCHEMA :: [ACTIVE] TO [BANK_USER];
GRANT DELETE ON SCHEMA :: [ACTIVE] TO [BANK_USER];
GRANT INSERT ON SCHEMA :: [ACTIVE] TO [BANK_USER];
GRANT UPDATE ON SCHEMA :: [ACTIVE] TO [BANK_USER];
GO

-- All on both schemas
GRANT CONTROL ON SCHEMA :: [ACTIVE] TO [BANK_ADMIN];
GRANT CONTROL ON SCHEMA :: [HISTORY] TO [BANK_ADMIN];
GO



/*
   7 - Grant rights at database level
*/

-- Which database to use.
USE [BANKING01]
GO

-- Add to database read / write roles
EXEC sp_addrolemember 'db_datareader', 'BANK_USER'
EXEC sp_addrolemember 'db_datawriter', 'BANK_USER'
GO

-- Add to database owner role 
EXEC sp_addrolemember 'db_owner', 'BANK_ADMIN'
GO



/*
   8 - Create sequence object for customer table
*/


-- Delete existing sequence.
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'SEQ_CUSTOMERS')
    DROP SEQUENCE [ACTIVE].[SEQ_CUSTOMERS]
GO

-- Add new sequence.
CREATE SEQUENCE [ACTIVE].[SEQ_CUSTOMERS]
    AS INT
    START WITH 100
    INCREMENT BY 1
    MINVALUE 1
    NO MAXVALUE 
    NO CYCLE
    NO CACHE;
GO

-- Show the new sequence
SELECT * FROM sys.sequences WHERE name = 'SEQ_CUSTOMERS' ;
GO



/*  
	9 - Create a active customer table
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[CUSTOMER]') AND type in (N'U'))
DROP TABLE [ACTIVE].[CUSTOMER]
GO

-- Add new table
CREATE TABLE [ACTIVE].[CUSTOMER]
(
   CUST_ID int not null,
   CUST_LNAME varchar(40),
   CUST_FNAME varchar(40),
   CUST_PHONE char(12),
   CUST_ADDRESS varchar(40),
   CUST_CITY varchar(40),
   CUST_STATE char(2),
   CUST_ZIP char(5) not null
);

-- Delete existing constraint
IF EXISTS (SELECT name FROM sys.objects WHERE name = N'PK_CUSTOMER' and type = 'PK')
	ALTER TABLE [ACTIVE].[CUSTOMER] DROP CONSTRAINT PK_CUSTOMER
GO

-- Add primary key constraint
ALTER TABLE [ACTIVE].[CUSTOMER] 
    ADD CONSTRAINT PK_CUSTOMER PRIMARY KEY CLUSTERED (CUST_ID);
GO


-- Delete existing constraint
IF EXISTS (SELECT * FROM sys.objects WHERE name = N'DF_CUST_ID' and type = 'D')
	ALTER TABLE [ACTIVE].[CUSTOMER] DROP CONSTRAINT DF_CUST_ID
GO

-- Add default constraint
ALTER TABLE [ACTIVE].[CUSTOMER] 
    ADD CONSTRAINT DF_CUST_ID DEFAULT (NEXT VALUE FOR ACTIVE.SEQ_CUSTOMERS)
	FOR CUST_ID;
GO

-- Show user defined objects
SELECT * FROM sys.objects WHERE is_ms_shipped = 0;
GO

  
/*  
	10 - Add sample records (25K)
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[CUSTOMER];

-- Load from pipe delimited file
BULK INSERT [BANKING01].[ACTIVE].[CUSTOMER]
   FROM 'C:\HSS\CUSTOMERS.TXT'
   WITH
     (
	    KEEPIDENTITY,
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRE_TRIGGERS
      );

-- Show data in the table
SELECT * FROM [ACTIVE].[CUSTOMER];
GO



/*
   11 - Create sequence object for accounts table
*/

-- Delete existing sequence.
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'SEQ_ACCOUNTS')
    DROP SEQUENCE [ACTIVE].[SEQ_ACCOUNTS]
GO

-- Add new sequence.
CREATE SEQUENCE [ACTIVE].[SEQ_ACCOUNTS]
    AS INT
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    NO MAXVALUE 
    NO CYCLE
    NO CACHE;
GO

-- Show the new sequence
SELECT * FROM sys.sequences WHERE name = 'SEQ_ACCOUNTS' ;
GO



/*  
	12 - Create a account table (checking/savings)
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[ACCOUNTS]') AND type in (N'U'))
DROP TABLE [ACTIVE].[ACCOUNTS]
GO

-- Add new table
CREATE TABLE [ACTIVE].[ACCOUNTS]
(
   ACCT_ID INT NOT NULL ,
   ACCT_TRANSIT_NO CHAR(9) NOT NULL,
   ACCT_ROUTING_NO VARCHAR(10) NOT NULL,
   ACCT_TYPE CHAR(1) NOT NULL,
   CUST_ID INT,
   CONSTRAINT PK_ACCOUNTS PRIMARY KEY CLUSTERED (ACCT_ID),
   CONSTRAINT CK_ACCT_TYPE CHECK (ACCT_TYPE IN ('C', 'S')),
);

-- Delete existing constraint
IF EXISTS (SELECT * FROM sys.objects WHERE name = N'DF_ACCT_ID' and type = 'D')
	ALTER TABLE [ACTIVE].[ACCOUNTS] DROP CONSTRAINT DF_ACCT_ID
GO

-- Add default constraint
ALTER TABLE [ACTIVE].[ACCOUNTS] 
    ADD CONSTRAINT DF_ACCT_ID DEFAULT (NEXT VALUE FOR ACTIVE.SEQ_ACCOUNTS)
	FOR ACCT_ID;
GO
   
-- Show user defined objects
SELECT * FROM sys.objects WHERE is_ms_shipped = 0;
GO



/*  
	13 - Add sample records 
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[ACCOUNTS];

-- Create some accounts
INSERT INTO [ACTIVE].[ACCOUNTS] ([ACCT_TRANSIT_NO], [ACCT_ROUTING_NO], [ACCT_TYPE], [CUST_ID])
SELECT 
    STR(100000 + (ABS(CHECKSUM(NEWID())) % 100000), 6, 0) +
    STR(100 + (ABS(CHECKSUM(NEWID())) % 100), 3, 0) AS [ACCT_TRANSIT_NO],
    STR(100000 + (ABS(CHECKSUM(NEWID())) % 100000), 6, 0) +
    STR(1000 + (ABS(CHECKSUM(NEWID())) % 1000), 4, 0) AS [ACCT_ROUTING_NO],
	CASE ABS(CHECKSUM(NEWID())) % 2
	    WHEN 1 THEN 'S'
		ELSE 'C'
	END AS ACCT_TYPE,
    CUST_ID 
FROM [ACTIVE].[CUSTOMER]
GO

-- Show data in the table
SELECT * FROM [ACTIVE].[ACCOUNTS];
GO



/*  
	14 - Try to enforce referential integrity 
*/

-- Delete existing constraint
IF EXISTS (SELECT * FROM sys.objects WHERE name = N'FK_ACCOUNT_2_CUSTOMER' and type = 'F')
	ALTER TABLE [ACTIVE].[ACCOUNTS] DROP CONSTRAINT FK_ACCOUNT_2_CUSTOMER
GO

-- Add foreign key constraint
ALTER TABLE [ACTIVE].[ACCOUNTS] WITH NOCHECK 
    ADD CONSTRAINT FK_ACCOUNT_2_CUSTOMER FOREIGN KEY (CUST_ID) 
	REFERENCES [ACTIVE].[CUSTOMER] (CUST_ID);
GO

-- Try to validate data
ALTER TABLE [ACTIVE].[ACCOUNTS]
    WITH CHECK CHECK CONSTRAINT ALL;
GO



/*  
	15 - Speeding up queries 
*/

-- Show data between the two tables	
SELECT 
-- *
C.CUST_ID, A.ACCT_ID, A.ACCT_TYPE 
FROM  
    [ACTIVE].[CUSTOMER]	C INNER JOIN
	[ACTIVE].[ACCOUNTS]	A 
ON  C.CUST_ID = A.CUST_ID
GO

-- Estimated plan is two CI scans

-- Drop non-clustered index
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'IX_ACCOUNT_CUST_ID')
    DROP INDEX [IX_ACCOUNT_CUST_ID] ON [ACTIVE].[ACCOUNTS];
GO

-- Create index on foreign keys (best practice!)
CREATE NONCLUSTERED INDEX IX_ACCOUNT_CUST_ID ON [ACTIVE].[ACCOUNTS] (CUST_ID)
--INCLUDE (ACCT_TRANSIT_NO, ACCT_ROUTING_NO);
GO

-- Show data between the two tables	
SELECT 	C.*, A.ACCT_TRANSIT_NO, A.ACCT_ROUTING_NO 
FROM  
    [ACTIVE].[CUSTOMER]	C INNER JOIN
	[ACTIVE].[ACCOUNTS]	A --WITH (INDEX(IX_ACCOUNT_CUST_ID))	
ON  A.CUST_ID = C.CUST_ID
GO

-- Estimated plan 1 CI and 1 NC seek



/*  
	16 - Create a active transaction table
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[TRANSACTION]') AND type in (N'U'))
DROP TABLE [ACTIVE].[TRANSACTION]
GO

-- Add new table
CREATE TABLE [ACTIVE].[TRANSACTION]
(
   TRAN_ID INT NOT NULL IDENTITY(1,1) CONSTRAINT PK_TRANSACTIONS PRIMARY KEY CLUSTERED (TRAN_ID),
   TRAN_AMOUNT MONEY NOT NULL,
   TRAN_DATE DATETIME NOT NULL CONSTRAINT DF_TRAN_DATE DEFAULT (SYSDATETIME()),
   ACCT_ID INT,
   CONSTRAINT FK_TRANSACTION_2_ACCOUNT FOREIGN KEY (ACCT_ID) REFERENCES [ACTIVE].[ACCOUNTS] (ACCT_ID)
   ON DELETE CASCADE
   ON UPDATE SET NULL	 
);


-- NO ACTION | CASCADE | SET NULL | SET DEFAULT 

-- Show user defined objects
SELECT * FROM sys.objects WHERE is_ms_shipped = 0;
GO


/*  
	17 - Stored procedure - generate data by month
*/


/*  
	Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ACTIVE].[USP_MAKE_TRANSACTIONS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [ACTIVE].[USP_MAKE_TRANSACTIONS]
GO
 

/*  
	Create the new stored procedure.
*/

CREATE PROCEDURE [ACTIVE].[USP_MAKE_TRANSACTIONS]
    @VAR_GIVEN_MONTH DATETIME = '01-01-2014',
    @VAR_VERBOSE_IND TINYINT = 1
AS
BEGIN
    
    -- NO COUNTING
    SET NOCOUNT ON;
    
    -- DECLARE LOCAL VARIABLES 
    DECLARE @CUST_ID INT;
    DECLARE @ACCT_ID INT; 
	DECLARE @ACCT_TYPE CHAR(1); 
	DECLARE @N INT;
	DECLARE @R INT;
     
    -- USE CURSOR TO CREATE TRANSACTION DATA
    DECLARE CUR_OBJS CURSOR FOR
        SELECT 
		    C.CUST_ID, 
			A.ACCT_ID, 
			A.ACCT_TYPE 
        FROM  
            [ACTIVE].[CUSTOMER]	C INNER JOIN
	        [ACTIVE].[ACCOUNTS]	A 
        ON  C.CUST_ID = A.CUST_ID
        
    -- OPEN THE CURSOR
    OPEN CUR_OBJS;
    SET @R = 1;

    -- EXAMINE EACH ACCOUNT & CREATE TRANSACTIONS
    WHILE (1=1) 
    BEGIN

        -- GRAB A ROW OF INFORMATION
        FETCH NEXT FROM CUR_OBJS
            INTO @CUST_ID, @ACCT_ID, @ACCT_TYPE;

        -- CHECK FOR PROCESSING ERRORS
        IF (@@FETCH_STATUS < 0) 
            BREAK; 

		-- CHECKING OR SAVINGS?
		IF @ACCT_TYPE = 'S' 
			SET @N = 1 + ABS(CHECKSUM(NEWID()) % 20)
		ELSE
			SET @N = 10 + ABS(CHECKSUM(NEWID()) % 20)

		-- ADD DATA
        INSERT INTO [ACTIVE].[TRANSACTION] (TRAN_AMOUNT, TRAN_DATE, ACCT_ID)
        SELECT TOP (@N)
            -2000 + ABS(CHECKSUM(NEWID()) % 5000) AS TRAN_AMOUNT,
	         DATEADD(D, ABS(CHECKSUM(NEWID()) % 27), @VAR_GIVEN_MONTH) AS TRAN_DATE,
	         @ACCT_ID
        FROM sys.systypes
		 
		-- SHOW RESULTS
		SET @R = @R + 1;
		IF (@VAR_VERBOSE_IND = 1)
		BEGIN
		    PRINT 'ROW = ' + STR(@R, 5, 0) + ' ACCT ID = ' +  STR(@ACCT_ID, 5, 0) + ' N = ' + STR(@N, 2, 0)
			PRINT ' '
		END
    END

    -- CLOSE THE CURSOR
    CLOSE CUR_OBJS;

    -- RELEASE THE CURSOR
    DEALLOCATE CUR_OBJS;
        
END
GO



/*  
	18 - Add sample records 
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[TRANSACTION];
GO

-- 
-- RUN AS 1 BATCH
--

-- JAN 2014 TO NOV 2015
DECLARE @I INT = 0;
DECLARE @DT DATETIME;

WHILE (@I < 23)
BEGIN
    SELECT @DT = DATEADD(M, @I, '01-01-2014');
    EXEC [ACTIVE].[USP_MAKE_TRANSACTIONS] 
	    @VAR_GIVEN_MONTH = @DT, 
		@VAR_VERBOSE_IND = 1;
	SET @I = @I + 1;
END
GO

-- Show data in the table
SELECT * FROM [ACTIVE].[TRANSACTION];
GO



/*  
	19 - Looking at our data
*/

-- ~ 8.5 M TRANSACTIONS
SELECT COUNT(*) AS TOTAL_RECS 
FROM [ACTIVE].[TRANSACTION];
GO

-- DATA DISTRIBUTION
SELECT  
     YEAR(TRAN_DATE) * 100 + MONTH(TRAN_DATE) AS DATE_KEY,
	 A.ACCT_TYPE,
	 AVG([TRAN_AMOUNT]) AS AVG_AMOUNT,
	 SUM([TRAN_AMOUNT])/1000000 AS PORT_BAL_M 
FROM [ACTIVE].[ACCOUNTS] A
JOIN [ACTIVE].[TRANSACTION] T
ON A.ACCT_ID = T.ACCT_ID
GROUP BY
     YEAR(TRAN_DATE) * 100 + MONTH(TRAN_DATE),
	 A.ACCT_TYPE
ORDER BY 
     YEAR(TRAN_DATE) * 100 + MONTH(TRAN_DATE),
	 A.ACCT_TYPE
GO

/*  
	20 - Add the month of DEC 2015
*/

-- New data has been added
/*
    EXEC [ACTIVE].[USP_MAKE_TRANSACTIONS] 
	    @VAR_GIVEN_MONTH = '12-01-2015', 
		@VAR_VERBOSE_IND = 1;
*/