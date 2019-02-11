/******************************************************
 *
 * Name:         itegrity-issues-snessug-miner-dec2012.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     12-11-2012
 *     Purpose:  Two examples of a simple AUTOS database.
 *               First example has integrity issues.
 *               Second example uses TSQL to enforce integrity.
 *
 ******************************************************/


--
-- 1A - Create the sample database
--

-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'AUTOS')
DROP DATABASE [AUTOS]
GO

-- Add new databases.
CREATE DATABASE [AUTOS] ON PRIMARY
( NAME = N'AUTOS_DAT', FILENAME = N'C:\MSSQL\DATA\AUTOS-DAT.MDF' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 2MB)
LOG ON
( NAME = N'AUTOS_LOG', FILENAME = N'C:\MSSQL\LOG\AUTOS-LOG.LDF' , SIZE = 2MB , MAXSIZE = UNLIMITED , FILEGROWTH = 256KB)
GO


--
-- 1B - Create the bad sample tables
--

-- Use the database
USE [AUTOS];
GO

-- Create Makes Table
CREATE TABLE MAKES
(
MAKER_NM VARCHAR(10),
START_YR SMALLINT,
END_YR SMALLINT
);
GO

-- Add data
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
('Chevrolet', 1912, Null),
('Dodge', 1915, Null),
('Ford', 1903, Null),
('Lincoln', 1917, Null),
('Mercury', 1938, 2011);
GO


-- Create Models Table
CREATE TABLE MODELS
(
MAKER_NM VARCHAR(10),
MODEL_NM VARCHAR(10),
MODEL_YR SMALLINT,
MSRP SMALLMONEY
);
GO

-- Allow data truncation
SET ANSI_WARNINGS OFF;
GO

-- Add data
INSERT INTO MODELS (MAKER_NM, MODEL_NM, MODEL_YR, MSRP) VALUES
('Chevrolet', 'Corvette', 2013, 49600.00),
('Dodge', 'Ram 1500', 2013, 22589.99),
('Ford', 'Mustang Boss 302', 2013, 42200.00),
('Lincoln', 'Navigator', 2013, 57775.00),
('Mercury', 'Mariner', 2011, 23082.00)
GO


--
-- 1C - Issues with design
--

-- User Defined Constraint issue
--   Range 1903 .. YEAR()+1, START < END
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
('Edsel', 1957, -1956);
GO


-- Entity Constraint Issue
--   Should not be null
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
(NULL, 1956, 1957);
GO


-- Referential Constraint Issue
--   Unknown parent
INSERT INTO MODELS (MAKER_NM, MODEL_NM, MODEL_YR, MSRP) VALUES
('Fiat', '500', 2013, 19500.00);
GO


-- Entity Constraint Issue
--   Should not be null
INSERT INTO MODELS (MAKER_NM, MODEL_NM, MODEL_YR, MSRP) VALUES
(NULL, NULL, 2013, 29500.00);
GO


-- Any processes still connected?
sp_who2
kill 51



--
-- 2A - Create the sample database
--

-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'AUTOS')
DROP DATABASE [AUTOS]
GO

-- Add new databases.
CREATE DATABASE [AUTOS] ON PRIMARY
( NAME = N'AUTOS_DAT', FILENAME = N'C:\MSSQL\DATA\AUTOS-DAT.MDF' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 2MB)
LOG ON
( NAME = N'AUTOS_LOG', FILENAME = N'C:\MSSQL\LOG\AUTOS-LOG.LDF' , SIZE = 2MB , MAXSIZE = UNLIMITED , FILEGROWTH = 256KB)
GO



--
-- 2B - Create the sample tables with good contraints
--

-- Do not allow data truncation
SET ANSI_WARNINGS ON;
GO

-- Use the database
USE [AUTOS];
GO

-- Create makes table, add constraints with alter
CREATE TABLE [DBO].[MAKES]
(
MAKER_ID INT IDENTITY(1,1),
MAKER_NM VARCHAR(25),
START_YR SMALLINT,
END_YR SMALLINT 
);
GO

-- User defined constraint
ALTER TABLE MAKES
ADD CONSTRAINT CHK_START_YR CHECK (ISNULL(START_YR, 0) >= 1903 and ISNULL(START_YR, 0) <= YEAR(GETDATE()) + 1);
GO

-- User defined constraint
ALTER TABLE MAKES
ADD CONSTRAINT CHK_END_YR CHECK ( (END_YR IS NULL) OR (END_YR >= 1903 AND END_YR <= YEAR(GETDATE()) + 1) );
GO

-- Entity constraint - natural key
ALTER TABLE MAKES
ADD CONSTRAINT UNQ_MAKER_NM UNIQUE (MAKER_NM); 
GO

-- Entity constraint - surrogate key
ALTER TABLE MAKES
ADD CONSTRAINT PK_MAKER_ID PRIMARY KEY CLUSTERED (MAKER_ID);
GO


-- Create models table with inline constraints
CREATE TABLE [DBO].[MODELS]
(
MODEL_ID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
MODEL_NM VARCHAR(25) NOT NULL CONSTRAINT DF_MODEL_NM DEFAULT 'UNKNOWN' CONSTRAINT UNQ_MODEL_NM UNIQUE (MODEL_NM),
MODEL_YR SMALLINT CONSTRAINT CHK_MDOEL_YR CHECK (ISNULL(MODEL_YR, 0) >= 1903 and ISNULL(MODEL_YR,0) <= YEAR(GETDATE()) + 1),
MSRP SMALLMONEY NOT NULL CONSTRAINT DF_MSRP DEFAULT 0,
MAKER_ID INT NOT NULL CONSTRAINT FK_MAKES FOREIGN KEY (MAKER_ID)
    REFERENCES [DBO].[MAKES] (MAKER_ID)
);
GO


--
-- 2C - Add good data to the tables
--


-- Add data to makes
INSERT INTO [DBO].[MAKES] (MAKER_NM, START_YR, END_YR) VALUES
('Chevrolet', 1912, Null),
('Dodge', 1915, Null),
('Ford', 1903, Null),
('Lincoln', 1917, Null),
('Mercury', 1938, 2011);
GO

-- Review data
SELECT * FROM [DBO].[MAKES]
GO

-- Add data
INSERT INTO MODELS (MODEL_NM, MODEL_YR, MSRP, MAKER_ID) VALUES
('Corvette', 2013, 49600.00, 1),
('Ram 1500', 2013, 22589.99, 2),
('Mustang Boss 302', 2013, 42200.00, 3),
('Navigator', 2013, 57775.00, 4),
('Mariner', 2011, 23082.00, 5)
GO

-- Review data
SELECT * FROM [DBO].[MODELS]
GO


--
-- 2D - Add bad data to the tables
--


-- User Defined Constraint issue
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
('Edsel', 1957, -1956);
GO


-- Entity Constraint Issue (Unique allows one null, not constraint on START_YR < END_YR)
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
(NULL, 1956, 1957);
GO


-- Referential Constraint Issue
INSERT INTO MODELS (MODEL_NM, MODEL_YR, MSRP, MAKER_ID) VALUES
('Fiat 500', 2013, 19500.00, -1);
GO


-- Entity Constraint Issue (fixes null issue on natural key)
INSERT INTO MODELS (MODEL_NM, MODEL_YR, MSRP, MAKER_ID) VALUES
(NULL, 2013, 29500.00, 1);
GO



--
-- 2E - Triggers can be also used but happen after insert
--

-- Remove the one record
DELETE FROM [DBO].[MAKES] WHERE MAKER_ID = 7
GO


-- Create the new trigger.
CREATE TRIGGER [DBO].[TRG_ENFORCE_TIME_LINE] on [DBO].[MAKES]
FOR INSERT, UPDATE
AS

BEGIN

    -- declare local variable
    DECLARE @VAR_MSG VARCHAR(250);
    DECLARE @VAR_START INT;
    DECLARE @VAR_END INT;

    -- nothing to do?
    IF (@@rowcount = 0) RETURN;

    -- do not count rows
    SET NOCOUNT ON;

    -- inserted data
    IF NOT EXISTS (SELECT * FROM deleted) AND EXISTS (SELECT * FROM inserted)
	BEGIN
	    SELECT @VAR_START = ISNULL([I].[START_YR], -1),  @VAR_END = ISNULL([I].[END_YR], -1) FROM [inserted] AS I
	    IF (@VAR_END <> -1) AND (@VAR_START > @VAR_END)
		BEGIN
            SET @VAR_MSG = 'The [DBO].[MAKES] table must have a start year <= end year!  Insert action rolled back';
            ROLLBACK TRANSACTION;
            RAISERROR (@VAR_MSG, 15, 1);
            RETURN;
		END
    END

    -- updated data
    IF EXISTS (SELECT * FROM deleted) AND EXISTS (SELECT * FROM inserted)
	BEGIN
	    SELECT @VAR_START = ISNULL([I].[START_YR], -1),  @VAR_END = ISNULL([I].[END_YR], -1) FROM [inserted] AS I
	    IF (@VAR_END <> -1) AND (@VAR_START > @VAR_END)
		BEGIN
            SET @VAR_MSG = 'The [DBO].[MAKES] table must have a start year <= end year!  Update action rolled back';
            ROLLBACK TRANSACTION;
            RAISERROR (@VAR_MSG, 15, 1);
            RETURN;
		END
    END

END
GO

-- DML Trigger to enforce business rule
INSERT INTO MAKES (MAKER_NM, START_YR, END_YR) VALUES
('Fiat', 1957, 1956);
GO

-- DML Trigger to enforce business rule
UPDATE [DBO].[MAKES]
SET END_YR = 1903
WHERE MAKER_ID = 5;

