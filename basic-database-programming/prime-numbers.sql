/******************************************************
 *
 * Name:         prime-numbers.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-12-2012
 *     Purpose:  A program to generate prime numbers.
 * 
 ******************************************************/


/*  
	Create a database to hold the prime numbers
*/


-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'MATH')
 DROP DATABASE MATH
GO


-- Add new databases.
CREATE DATABASE MATH ON  
 PRIMARY 
  ( NAME = N'TRAN_PRI_DAT', FILENAME = N'C:\MSSQL\DATA\MATH_DATA.MDF' , SIZE = 32MB, FILEGROWTH = 16MB) 
 LOG ON 
  ( NAME = N'TRAN_ALL_LOG', FILENAME = N'C:\MSSQL\LOG\MATH_LOG.LDF' , SIZE = 16MB, FILEGROWTH = 16MB)
GO



/*  
	Create a table to hold the primes
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DBO].[TBL_PRIMES]') AND type in (N'U'))
DROP TABLE [DBO].[TBL_PRIMES]
GO


-- Add new table
CREATE TABLE [DBO].[TBL_PRIMES] (
	[MY_VALUE] [bigint] NOT NULL,
	[MY_TIME] [datetime] NOT NULL DEFAULT (GETDATE())
) 
GO

-- Delete existing index
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'IDX_PRIMES')
    DROP INDEX IDX_PRIMES ON [DBO].[TBL_PRIMES];
GO

-- Add new index
CREATE INDEX IDX_PRIMES 
    ON [DBO].[TBL_PRIMES] ([MY_VALUE]); 
GO



/*  
	Create a scalar value function to determine if number is prime
*/


-- Which database to use.
USE [MATH]
GO

-- Delete existing function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[IS_PRIME]') AND type in (N'FN'))
DROP FUNCTION [dbo].[IS_PRIME]
GO

-- Create the function from scratch
CREATE FUNCTION IS_PRIME (@VAR_NUM2 BIGINT)
RETURNS INT
AS
BEGIN
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

    -- NOT A PRIME NUMBER
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
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[STORE_PRIMES]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[STORE_PRIMES]
GO

-- Create the stored procedure from scratch
CREATE PROCEDURE STORE_PRIMES
    @VAR_ALPHA BIGINT,
    @VAR_OMEGA BIGINT
AS
BEGIN
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
        SELECT @VAR_RET1 = [DBO].[IS_PRIME] (@VAR_CNT1);
        
        -- FOUND A PRIME
        IF (@VAR_RET1 = 1)
          BEGIN
            INSERT INTO [DBO].[TBL_PRIMES] (MY_VALUE) VALUES (@VAR_CNT1);
            PRINT @VAR_CNT1;
          END        
        ELSE        
          BEGIN
            PRINT '...';
          END;
        
        -- INCREMENT COUNTER
        SELECT @VAR_CNT1 = @VAR_CNT1 + 1        
    END;
    
END
GO


/*  
	Calculate primes between 1 & 750K
*/



EXEC STORE_PRIMES 1, 750000




/*  
	Total time in seconds (37)
*/

SELECT DATEDIFF(s, DT.START, DT.FINISH) AS TOTAL
FROM (SELECT MIN(MY_TIME) AS START, MAX(MY_TIME) AS FINISH FROM dbo.TBL_PRIMES) DT


SELECT top 5 * FROM dbo.TBL_PRIMES DT order by MY_VALUE desc



/*  
	Create a view see primes from 100K to 199K
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing procedure
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[VW_ONE_HUNDRED_K_PRIMES]') AND type = 'V')
DROP VIEW [dbo].[VW_ONE_HUNDRED_K_PRIMES]
GO

-- Create the stored procedure from scratch
CREATE VIEW [dbo].[VW_ONE_HUNDRED_K_PRIMES]
AS
    SELECT * 
	FROM [dbo].[TBL_PRIMES] 
	WHERE [MY_VALUE] > 100000 AND [MY_VALUE] < 200000
GO



/*  
	Timings (view vs adhoc)
*/

-- Show time & i/o
SET STATISTICS TIME ON
SET STATISTICS IO ON
GO

-- Remove clean buffers & clear plan cache
CHECKPOINT 
DBCC DROPCLEANBUFFERS 
DBCC FREEPROCCACHE
GO


-- Select from view (compiled)
SELECT count(*) as TOTAL
FROM [dbo].[VW_ONE_HUNDRED_K_PRIMES]

/*
SQL Server parse and compile time: 
   CPU time = 0 ms, elapsed time = 22 ms.

(1 row(s) affected)
Table 'TBL_PRIMES'. Scan count 1, logical reads 25, physical reads 1, 
read-ahead reads 23, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 10 ms.
*/


SELECT count(*) as TOTAL
FROM [dbo].[TBL_PRIMES] 
WHERE [MY_VALUE] > 100000 AND [MY_VALUE] < 200000


/*

SQL Server parse and compile time: 
   CPU time = 0 ms, elapsed time = 42 ms.

SQL Server parse and compile time: 
   CPU time = 0 ms, elapsed time = 0 ms.

(1 row(s) affected)
Table 'TBL_PRIMES'. Scan count 1, logical reads 25, physical reads 1, 
read-ahead reads 23, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.

 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 10 ms.

*/

-- Dont show time & i/o
SET STATISTICS TIME OFF
SET STATISTICS IO OFF
GO


/*  
    Table valued function (TVF)

	Create a function to return a table of primes from x to y
*/

-- Which database to use.
USE [MATH]
GO

-- Delete existing procedure
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FN_GET_PRIMES_BY_RANGE]') AND type = 'IF')
DROP FUNCTION [dbo].[FN_GET_PRIMES_BY_RANGE]
GO

-- Create the function from scratch
CREATE FUNCTION [dbo].[FN_GET_PRIMES_BY_RANGE] (@VAR_START bigint, @VAR_END bigint)
RETURNS TABLE
AS
RETURN 
(
    SELECT [MY_VALUE]
    FROM [dbo].[TBL_PRIMES]
    WHERE [MY_VALUE] > @VAR_START AND [MY_VALUE] < @VAR_END 
);
GO

-- CALL TABLE VALUE FUNCTION
SELECT * FROM [dbo].[FN_GET_PRIMES_BY_RANGE] (1, 1000)