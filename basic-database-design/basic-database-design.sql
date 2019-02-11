/******************************************************
 *
 * Name:         basic-database-design.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-11-2014
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
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING')
 DROP DATABASE [BANKING]
GO


-- Add new databases.
CREATE DATABASE [BANKING] ON  
 PRIMARY 
  ( NAME = N'BANKING_PRI_DAT', FILENAME = N'C:\MSSQL\DATA\BANKING.MDF' , SIZE = 32MB, FILEGROWTH = 16MB) 
 LOG ON 
  ( NAME = N'BANKING_ALL_LOG', FILENAME = N'C:\MSSQL\LOG\BANKING.LDF' , SIZE = 32MB, FILEGROWTH = 16MB)
GO



/*  
	2 - Add file group to spread i/o
*/

-- Add new file group
ALTER DATABASE [BANKING] ADD FILEGROUP FG_MULTI_FILE
GO

-- Add files to spread i/o
ALTER DATABASE [BANKING]
ADD FILE
( NAME = N'BANKING_DAT01', FILENAME = N'C:\MSSQL\DATA\BANKING-01.NDF' , SIZE = 32MB, FILEGROWTH = 16MB)
TO FILEGROUP FG_MULTI_FILE;
GO

-- Add files to spread i/o
ALTER DATABASE [BANKING]
ADD FILE
( NAME = N'BANKING_DAT02', FILENAME = N'C:\MSSQL\DATA\BANKING-02.NDF' , SIZE = 32MB, FILEGROWTH = 16MB)
TO FILEGROUP FG_MULTI_FILE;
GO

-- Add files to spread i/o
ALTER DATABASE [BANKING]
ADD FILE
( NAME = N'BANKING_DAT03', FILENAME = N'C:\MSSQL\DATA\BANKING-03.NDF' , SIZE = 32MB, FILEGROWTH = 16MB)
TO FILEGROUP FG_MULTI_FILE;
GO

-- Add files to spread i/o
ALTER DATABASE [BANKING]
ADD FILE
( NAME = N'BANKING_DAT04', FILENAME = N'C:\MSSQL\DATA\BANKING-04.NDF' , SIZE = 32MB, FILEGROWTH = 16MB)
TO FILEGROUP FG_MULTI_FILE;
GO


-- No need to specify [ON] clause when creating objects (tables/indexes)
ALTER DATABASE [BANKING] 
MODIFY FILEGROUP FG_MULTI_FILE DEFAULT;
GO

-- Must supply [ON] clause
ALTER DATABASE [BANKING]
MODIFY FILEGROUP [PRIMARY] DEFAULT;
GO



/*
   3 - Create Active schema
*/

-- Which database to use.
USE [BANKING]
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
   4 - Create history schema
*/

-- Which database to use.
USE [BANKING]
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
   5 - Create server logins (admin/user)
*/

-- Which database to use.
USE [master]
GO


-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'BANK_ADMIN')
DROP LOGIN [BANK_ADMIN]
GO

-- Add new login.
CREATE LOGIN [BANK_ADMIN] WITH PASSWORD=N'M0a2r0c9h11$', DEFAULT_DATABASE=[BANKING]
GO


-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'BANK_USER')
DROP LOGIN [BANK_USER]
GO

-- Add new login.
CREATE LOGIN [BANK_USER] WITH PASSWORD=N'A0p4r0i7l15#', DEFAULT_DATABASE=[BANKING]
GO



/*
   6 - Create database user (admin/user)
*/
  
-- Which database to use.
USE [BANKING]
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
CREATE USER [BANK_USER] FOR LOGIN [BANK_USER] WITH DEFAULT_SCHEMA=[DBO]
GO

-- Change default schema 
--CREATE USER [BANK_USER] FOR LOGIN [BANK_USER] WITH DEFAULT_SCHEMA=[ACTIVE]



/*
   7 - Grant rights at schema level
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
   8 - Grant rights at database level
*/

-- Which database to use.
USE [BANKING]
GO

-- Add to database owner role 
EXEC sp_addrolemember 'db_owner', 'BANK_ADMIN'
GO

-- Add to database read / write roles
EXEC sp_addrolemember 'db_datareader', 'BANK_USER'
EXEC sp_addrolemember 'db_datawriter', 'BANK_USER'
GO



/*
   9 - Create sequence object for customer table
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
	10 - Create a active customer table
*/

-- Which database to use.
USE [BANKING]
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
	11 - Add sample records - Avengers
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[CUSTOMER];

-- Iron Man
INSERT INTO [ACTIVE].[CUSTOMER] VALUES
    (DEFAULT, 'Downey Jr.', 'Robert', '424-288-2000', '1311 Abbot Kinney', 'Venice', 'CA', '90291');
GO

-- Black Widow
INSERT INTO [ACTIVE].[CUSTOMER] VALUES
    (DEFAULT, 'Johansson', 'Scarlett', '310-774-0200', '1017 Ocean Avenue, Suite G', 'Santa Monica', 'CA', '90403');
GO

-- Captain America
INSERT INTO [ACTIVE].[CUSTOMER] VALUES
    (DEFAULT, 'Evans', 'Chris', '310-888-3200', '9460 Wilshire Blvd, 7th Floor', 'Beverly Hills', 'CA', '90212');
GO

-- The Hulk
INSERT INTO [ACTIVE].[CUSTOMER] VALUES
    (DEFAULT, 'Ruffalo', 'Mark', '310-889-0300', '9150 Wilshire Blvd, Suite 350', 'Beverly Hills', 'CA', '90212');
GO

-- Thor
INSERT INTO [ACTIVE].[CUSTOMER] VALUES
    (DEFAULT, 'Hemsworth', 'Chris', '310-889-0300', '16 Princess Avenue', 'Rosebery, NSW 2018, Australia', '', '');
GO

-- Show data in the table
SELECT * FROM [ACTIVE].[CUSTOMER];
GO



/*
   12 - Create sequence object for accounts table
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
	13 - Create a account table (checking/savings)
*/

-- Which database to use.
USE [BANKING]
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
	14 - Add sample records 
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[ACCOUNTS];

-- Iron Man
INSERT INTO [ACTIVE].[ACCOUNTS] VALUES
    (DEFAULT, '000111000', '0123456789', 'C', 1)
GO

-- Black Widow
INSERT INTO [ACTIVE].[ACCOUNTS] VALUES
    (DEFAULT, '000222000', '0123456789', 'S', 2)
GO

-- Captain America
INSERT INTO [ACTIVE].[ACCOUNTS] VALUES
    (DEFAULT, '000333000', '0123456789', 'C', 3)
GO

-- The Hulk
INSERT INTO [ACTIVE].[ACCOUNTS] VALUES
    (DEFAULT, '000444000', '0123456789', 'S', 4)
GO

-- Thor
INSERT INTO [ACTIVE].[ACCOUNTS] VALUES
    (DEFAULT, '000555000', '0123456789', 'S', 5)
GO


-- Show data in the table
SELECT * FROM [ACTIVE].[ACCOUNTS];
GO



/*  
	15 - Try to enforce referential integrity 
*/


-- Delete existing constraint
IF EXISTS (SELECT * FROM sys.objects WHERE name = N'FK_ACCOUNT_2_CUSTOMER' and type = 'F')
	ALTER TABLE [ACTIVE].[ACCOUNTS] DROP CONSTRAINT FK_ACCOUNT_2_CUSTOMER
GO

-- Add foreign key constraint
ALTER TABLE [ACTIVE].[ACCOUNTS] WITH NOCHECK 
 ADD CONSTRAINT FK_ACCOUNT_2_CUSTOMER FOREIGN KEY (CUST_ID) REFERENCES [ACTIVE].[CUSTOMER] (CUST_ID);
GO

-- Try to validate data
ALTER TABLE [ACTIVE].[ACCOUNTS]
      WITH CHECK CHECK CONSTRAINT ALL;
GO

-- Alter the sequence.
ALTER SEQUENCE [ACTIVE].[SEQ_CUSTOMERS] RESTART WITH 1;
GO


-- Remove data
TRUNCATE TABLE 

-- Drop foreign key
-- Reload the data (Step 11)
-- Recreate key



/*  
	16 - Speeding up queries 
*/

-- Show data between the two tables	
SELECT 	* 
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

-- Create index
CREATE NONCLUSTERED INDEX IX_ACCOUNT_CUST_ID ON [ACTIVE].[ACCOUNTS] (CUST_ID);
GO

-- Show data between the two tables	
SELECT 	* 
FROM  
    [ACTIVE].[CUSTOMER]	C INNER JOIN
	[ACTIVE].[ACCOUNTS]	A 
	WITH (INDEX(IX_ACCOUNT_CUST_ID))
ON  C.CUST_ID = A.CUST_ID
GO

-- Estimated plan 1 CI and 1 NC seek



/*  
	17 - Create a active transaction table
*/

-- Which database to use.
USE [BANKING]
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
) ON [FG_MULTI_FILE];


-- NO ACTION | CASCADE | SET NULL | SET DEFAULT 

-- Show user defined objects
SELECT * FROM sys.objects WHERE is_ms_shipped = 0;
GO



/*  
	18 - Add sample records 
*/

-- Clear the table
TRUNCATE TABLE [ACTIVE].[TRANSACTION];

-- Iron Man
INSERT INTO [ACTIVE].[TRANSACTION] VALUES
    (500, DEFAULT, 1)
GO

-- Black Widow
INSERT INTO [ACTIVE].[TRANSACTION] VALUES
    (1000, DEFAULT, 2)
GO

-- Captain America
INSERT INTO [ACTIVE].[TRANSACTION] VALUES
    (1500, DEFAULT, 3)
GO

-- The Hulk
INSERT INTO [ACTIVE].[TRANSACTION] VALUES
    (2000, DEFAULT, 4)
GO

-- Thor
INSERT INTO [ACTIVE].[TRANSACTION] VALUES
    (2500, DEFAULT, 5)
GO


-- Show data in the table
SELECT * FROM [ACTIVE].[TRANSACTION];
GO



/*  
	17 - Test referential integrity	on clauses
*/


-- Show the account data
SELECT * FROM [ACTIVE].[ACCOUNTS]
GO

-- Show the transaction data
SELECT * FROM [ACTIVE].[TRANSACTION]
GO

-- Show data in the table
UPDATE [ACTIVE].[ACCOUNTS]
SET ACCT_ID = 6
WHERE ACCT_ID = 2
GO

-- Show data in the table
DELETE
FROM [ACTIVE].[ACCOUNTS]
WHERE ACCT_ID = 3
GO



/*  
	18 - Filtered index
*/

-- Drop non-clustered index
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'IX_ACCOUNT_TYPE')
    DROP INDEX [IX_ACCOUNT_TYPE] ON [ACTIVE].[ACCOUNTS];
GO

-- Create index
CREATE NONCLUSTERED INDEX [IX_ACCOUNT_TYPE] ON [ACTIVE].[ACCOUNTS] (ACCT_TYPE) WHERE ACCT_TYPE = 'C';
GO


-- Use the filtered 
SELECT * FROM  [ACTIVE].[ACCOUNTS] 
WITH (INDEX(IX_ACCOUNT_TYPE))
WHERE ACCT_TYPE = 'C'



/*  
	19 - Covered index
*/

-- Drop non-clustered index
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'IX_ACCOUNT_TYPE_ABA')
    DROP INDEX [IX_ACCOUNT_TYPE_ABA] ON [ACTIVE].[ACCOUNTS];
GO

-- Create index
CREATE NONCLUSTERED INDEX [IX_ACCOUNT_TYPE_ABA] ON [ACTIVE].[ACCOUNTS] (ACCT_TYPE) INCLUDE (ACCT_TRANSIT_NO, ACCT_ROUTING_NO)
GO

-- Use the filtered 
SELECT ACCT_TRANSIT_NO, ACCT_ROUTING_NO 
FROM  [ACTIVE].[ACCOUNTS] 
--WITH (INDEX(IX_ACCOUNT_TYPE))
WHERE ACCT_TYPE = 'C'
