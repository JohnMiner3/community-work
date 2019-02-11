/******************************************************
 *
 * Name:         variables-n-data-types.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-20-2014
 *     Purpose:  Declare variables and demonstrate 
 *               control flow statements.
 * 
 ******************************************************/

-- Use model database
USE Model
GO


-- Data types (int, real, varchar, datetime)
-- http://msdn.microsoft.com/en-us/library/ms187752.aspx


-- 
-- Integer variable
--

-- Declare variable
DECLARE @var_int INT = 0;

-- Assign value using SELECT
SELECT @var_int = 2;

-- Show values
PRINT '';
PRINT 'EX 1 -- INTEGER VARIABLE';
PRINT 'BASE';
PRINT @var_int;
PRINT 'RAISED TO 4TH POWER EQUALS';

-- Assign value using SET
SET @var_int = POWER(@var_int, 4);
PRINT @var_int;
GO


-- 
-- Floating point variable
--

-- Declare variable
DECLARE @var_real REAL = 0;

-- Assign value using SELECT
SELECT @var_real = 15.625;

-- Show values
PRINT '';
PRINT 'EX 2 -- FLOATING POINT VARIABLE';
PRINT 'BASE';
PRINT @var_real;
PRINT 'RAISED TO 1/3 RD POWER EQUALS';

-- Assign value using SET
SET @var_real = POWER(@var_real, .333333);
PRINT @var_real;
GO


-- 
-- String variable
--

-- Declare variable
DECLARE @var_str NVARCHAR(255) = '';

-- Assign value using SELECT
SELECT @var_str = 'The cat ';
SET @var_str += 'in the hat!'

-- Show values
PRINT '';
PRINT 'EX 3 -- STRING VARIABLE';
PRINT @var_str;
GO


-- 
-- Date/time variable
--

-- Declare variable
DECLARE @var_dt DATETIME;

-- Assign value using SELECT
SELECT @var_dt = GETDATE();

-- Show values
PRINT '';
PRINT 'EX 4 -- DATE/TIME VARIABLE';
PRINT @var_dt;
GO


-- 
-- Control flow
--

-- http://msdn.microsoft.com/en-us/library/ms174290(v=sql.110).aspx

-- Declare variable
DECLARE @var_cnt INT = 0;

-- Another example
PRINT ' '
PRINT 'EX 5 -- CONTROL FLOW';

-- A loop with conditions
WHILE (@var_cnt < 11)
BEGIN

    -- Increment the count
    SET @var_cnt += 1;

	-- Even or odd
	IF ((@var_cnt % 2) = 0)
	    PRINT STR(@var_cnt, 2, 0) + ' is even.'
	ELSE
	    PRINT STR(@var_cnt, 2, 0) + ' is odd.';
END
GO


-- 
-- Store results from SELECT
--

-- Use adventureworks database
USE [AdventureWorks2012]
GO

-- Declare variable
DECLARE @var_sales MONEY = 0;

-- Another example
PRINT ' '
PRINT 'EX 6 -- DML OUTPUT';

-- Total sales (US)
SELECT 
    @var_sales = SUM(P.SalesYTD) 
FROM 
    Sales.SalesPerson P 
INNER JOIN 
    Sales.SalesTerritory T ON P.TerritoryID = T.TerritoryID
WHERE 
    T.CountryRegionCode = 'US'
GROUP BY 
    T.CountryRegionCode;

-- Total US Sales Ytd
PRINT 'Total US Sales';
PRINT @var_sales;


-- 
-- Continue vs Break (MSDN example)
--

-- Make a backup
/*
SELECT * INTO STAGE.Product FROM Production.Product
GO
*/

-- Use adventureworks database
USE [AdventureWorks2012]
GO


-- Update back to saved value
UPDATE P
SET P.ListPrice = S.ListPrice
FROM Production.Product P
JOIN STAGE.Product S ON P.ProductID = S.ProductID
GO

-- Show non-zero values
SELECT P.ProductID, P.ListPrice, S.ListPrice
FROM Production.Product P
JOIN STAGE.Product S ON P.ProductID = S.ProductID
WHERE P.ListPrice > 0
GO

-- Sample break/continue
WHILE (SELECT AVG(ListPrice) FROM Production.Product) < $500
BEGIN
   UPDATE Production.Product
      SET ListPrice = ListPrice * 2;
   SELECT AVG(ListPrice) FROM Production.Product;
   IF (SELECT AVG(ListPrice) FROM Production.Product) > $500
      BREAK
   ELSE
      CONTINUE
END
PRINT 'Too much for the market to bear';
GO