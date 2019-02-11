/******************************************************
 *
 * Name:         sample-view.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-12-2012
 *     Purpose:  Demonstrate how to create, alter and 
 *               drop a view.
 * 
 ******************************************************/

/*

To recap this article, typical uses of a view are the following:

1 - To simplify or customize the perception each user has of the database.
2 - Security mechanism to grant users access to the view, not the underlying base tables.
3 - To provide a backward compatible interface to emulate a table whose schema has changed.

*/

 
-- Use the correct database
USE [AdventureWorks2012]
GO

-- Delete existing view
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[HumanResources].[vHireDate]') AND [type] in (N'V')
)
DROP VIEW [HumanResources].[vHireDate]
GO

-- Create stub view (abstracting columns)
CREATE VIEW [HumanResources].[vHireDate] (Eenie, Meenie, Miney, Mo)
AS
    SELECT 1 AS Fee, 2 AS Fi, '3' AS Fo, '4' AS Fumb
GO

-- Show the funny data
SELECT * FROM [HumanResources].[vHireDate]
GO



-- Alter view to msdn example
ALTER VIEW [HumanResources].[vHireDate] 
AS 
    SELECT 
        p.FirstName
      , p.LastName
      , e.BusinessEntityID
      , e.HireDate
    FROM 
        HumanResources.Employee e 
        JOIN Person.Person AS p ON e.BusinessEntityID = p.BusinessEntityID;
GO

-- Example use of the view
SELECT * 
FROM [HumanResources].[vHireDate] 
WHERE FirstName like 'J%'
ORDER BY FirstName
GO

-- Update the view
UPDATE HD
SET FirstName = 'Jackie'
FROM [HumanResources].[vHireDate] HD
WHERE HD.BusinessEntityId = 82
GO