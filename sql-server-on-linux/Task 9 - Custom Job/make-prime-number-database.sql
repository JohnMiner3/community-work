/******************************************************
 *
 * Name:         make-prime-number-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-10-2016
 *     Purpose:  Calculate & store prime numbers.
 * 
 ******************************************************/

/*  
 Create a database to hold the prime numbers
*/

-- Which database to use.
USE [master]
GO

-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'MATH')
DROP DATABASE MATH
GO

-- Create new database
CREATE DATABASE MATH
 ON PRIMARY 
  ( NAME = N'MATH_PRI_DAT', FILENAME = N'/var/opt/mssql/data/math.mdf' , SIZE = 128MB, FILEGROWTH = 64MB) 
 LOG ON 
  ( NAME = N'MATH_ALL_LOG', FILENAME = N'/var/opt/mssql/data/math.ldf' , SIZE = 32MB, FILEGROWTH = 32MB)
GO
 


/*  
 Create a table to hold the prime numbers
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects 
  WHERE object_id = OBJECT_ID(N'[DBO].[TBL_PRIMES]') AND type in (N'U'))
DROP TABLE [DBO].[TBL_PRIMES]
GO

-- Add new table
CREATE TABLE [DBO].[TBL_PRIMES] 
(
  [MY_VALUE] [bigint] NOT NULL,
  [MY_DIVISION] [bigint] NOT NULL CONSTRAINT [CHK_TBL_PRIMES] CHECK ([MY_DIVISION] > 0),
  [MY_TIME] [datetime] NOT NULL CONSTRAINT [DF_TBL_PRIMES] DEFAULT (GETDATE())
  CONSTRAINT [PK_TBL_PRIMES] PRIMARY KEY CLUSTERED ([MY_VALUE] ASC)
) 
GO


/*  
 Create a procedure to determine if number is prime
*/


-- Which database to use.
USE [MATH]
GO

-- Delete existing procedure
IF  EXISTS (SELECT * FROM sys.objects 
  WHERE object_id = OBJECT_ID(N'[dbo].[SP_IS_PRIME]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SP_IS_PRIME]
GO

-- Create the stored procedure from scratch
CREATE PROCEDURE [dbo].[SP_IS_PRIME]
    @VAR_NUM2 BIGINT
AS
BEGIN
    -- NO DISPLAY
    SET NOCOUNT ON
 
    -- LOCAL VARIABLES
    DECLARE @VAR_CNT2 BIGINT;
    DECLARE @VAR_MAX2 BIGINT;

    -- NOT A PRIME NUMBER
    IF (@VAR_NUM2 = 1)
        RETURN 0;            

    -- A PRIME NUMBER
    IF (@VAR_NUM2 = 2)
        RETURN 1;            

    -- SET UP COUNTERS    
    SELECT @VAR_CNT2 = 2;
    SELECT @VAR_MAX2 = SQRT(@VAR_NUM2) + 1;

    -- TRIAL DIVISION 2 TO SQRT(X)
    WHILE (@VAR_CNT2 <= @VAR_MAX2)
    BEGIN
        -- NOT A PRIME NUMBER
        IF (@VAR_NUM2 % @VAR_CNT2) = 0
            RETURN 0;            

        -- INCREMENT COUNTER
        SELECT @VAR_CNT2 = @VAR_CNT2 + 1;
        
    END;

    -- A PRIME NUMBER
    RETURN 1;
    
END
GO


/*    
 Create a procedure to store primes from x to y.
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing procedure
IF  EXISTS (SELECT * FROM sys.objects 
  WHERE object_id = OBJECT_ID(N'[dbo].[SP_STORE_PRIMES]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SP_STORE_PRIMES]
GO

-- Create the stored procedure from scratch
CREATE PROCEDURE SP_STORE_PRIMES
    @VAR_ALPHA BIGINT,
    @VAR_OMEGA BIGINT
AS
BEGIN
    -- NO DISPLAY
    SET NOCOUNT ON
 
    -- DECLARE VARIABLES
    DECLARE @VAR_CNT1 BIGINT;
    DECLARE @VAR_RET1 INT;
    
    -- SET VARIABLES
    SELECT @VAR_RET1 = 0;
    SELECT @VAR_CNT1 = @VAR_ALPHA;

    -- CHECK EACH NUMBER FOR PRIMENESS
    WHILE (@VAR_CNT1 <= @VAR_OMEGA)
    BEGIN
        -- ARE WE PRIME?
        EXEC @VAR_RET1 = DBO.SP_IS_PRIME @VAR_CNT1;
        
        -- FOUND A PRIME
        IF (@VAR_RET1 = 1)
          INSERT INTO [DBO].[TBL_PRIMES] (MY_VALUE, MY_DIVISION) 
    VALUES (@VAR_CNT1, SQRT(@VAR_CNT1));
    
        -- INCREMENT COUNTER
        SELECT @VAR_CNT1 = @VAR_CNT1 + 1        
    END;
    
END
GO


/*  
 Create a table to hold the job control card
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects 
  WHERE object_id = OBJECT_ID(N'[DBO].[TBL_CONTROL_CARD]') AND type in (N'U'))
DROP TABLE [DBO].[TBL_CONTROL_CARD]
GO

-- Add new table
CREATE TABLE [DBO].[TBL_CONTROL_CARD] 
(
  [MY_VALUE] [bigint] NOT NULL
) 
GO

-- Start at 1
INSERT INTO [DBO].[TBL_CONTROL_CARD] VALUES (1);


/*    
    Show database objects
*/

SELECT *
FROM sys.objects
WHERE is_ms_shipped = 0;


/*    
    Show database objects
*/

exec sp_spaceused 'TBL_PRIMES';