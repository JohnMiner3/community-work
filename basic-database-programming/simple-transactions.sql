/******************************************************
 *
 * Name:         transactions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-20-2014
 *     Purpose:  Show how to implement simple transactions.
 * 
 ******************************************************/

-- Use model database
USE Model
GO

-- 
-- Example from MSDN
--

-- Use adventureworks database
USE [AdventureWorks2012]
GO

-- Auto commit is the default transaction mode.

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


-- Rollback vs Commit
BEGIN TRAN

  UPDATE Production.Product
  SET ListPrice = ListPrice * 2;

  IF (SELECT AVG(ListPrice) FROM Production.Product) > 500
    BEGIN
      PRINT 'DO NOT COMMIT CHANGES';
      ROLLBACK TRAN;
    END
  ELSE
    BEGIN
      PRINT 'COMMIT CHANGES';
      COMMIT TRAN;
    END
