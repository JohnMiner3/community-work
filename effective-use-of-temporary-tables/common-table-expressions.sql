/******************************************************
 *
 * Name:         common-table-expressions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     10-09-2012
 *     Purpose:  Samples of common table expressions.
 * 
 ******************************************************/


--
-- Auto database (select example)
--

-- Database selection does not matter
USE model;
GO

-- Create a list & show contents
WITH CTE_LIST(MyId, MyValue)
AS
(
SELECT * FROM
  (
    VALUES
    (1, 'Continental'),
    (2, 'Edsel'),
    (3, 'Lincoln'),
    (4, 'Mercury'),
    (5, 'Ram')
  ) AS A (MyId, MyValue)
)
SELECT * FROM CTE_LIST;
GO


--
-- AW database (select example)
--

-- Use the correct database
USE AdventureWorks2012
GO

-- Get customer name, sales id, sub-total and order date
WITH CTE_MC (MyCustomer, MyDate)
AS
(
    SELECT
      CustomerID AS MyCustomer,
      MAX(OrderDate) AS MyDate
    FROM
      Sales.SalesOrderHeader
    WHERE
      YEAR(OrderDate) = 2008
    GROUP BY
  	  CustomerID
)

SELECT
  P.FirstName,
  P.LastName,
  SH.SalesOrderID,
  SH.SubTotal,
  SH.OrderDate
FROM
  Sales.SalesOrderHeader AS SH
  INNER JOIN CTE_MC AS MC ON SH.CustomerID = MC.MyCustomer AND SH.OrderDate = MC.MyDate
  INNER JOIN Sales.Customer C ON MC.MyCustomer = C.CustomerID
  INNER JOIN Person.Person P ON C.PersonID = P.BusinessEntityID
ORDER BY
  SH.OrderDate ASC;

-- Pause for a minute
WAITFOR DELAY '00:01';
GO


--
-- AW database (duplicate department table)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Remove department staging table
IF  EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[STAGE].[Department]') AND type in (N'U')
    )
    DROP TABLE [STAGE].[Department]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'STAGE')
DROP SCHEMA STAGE
GO

-- Add new schema.
CREATE SCHEMA [STAGE] AUTHORIZATION [dbo]
GO

-- Recreate table from AW entries
SELECT * INTO [STAGE].[Department] FROM [HumanResources].[Department]
GO

-- Add a default date
ALTER TABLE [STAGE].[Department] ADD CONSTRAINT df_Modified_Date
  DEFAULT GETDATE() FOR [ModifiedDate];
GO

-- Show the data
SELECT * FROM [STAGE].[Department] 
GO


--
-- AW database (update example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Update group to US
WITH CTE_DEPT (GroupName)
AS
(
    SELECT GroupName FROM [STAGE].[Department]
    WHERE GroupName = 'Quality Assurance'
)
UPDATE CTE_DEPT SET GroupName = 'US Quality Assurance';
GO

-- Show the data
SELECT * FROM [STAGE].[Department] 
GO

--
-- AW database (insert example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Insert group for ASIA
WITH CTE_DEPT(Name, GroupName)
AS
(
      SELECT Name, GroupName
      FROM [STAGE].[Department]
      WHERE GroupName = 'US Quality Assurance'
)
INSERT INTO CTE_DEPT
SELECT D.Name, 'ASIA ' + SUBSTRING(D.GroupName, 4, LEN(D.GroupName) - 3)
FROM CTE_DEPT AS D
GO

-- Show the data
SELECT * FROM [STAGE].[Department] 
GO


--
-- AW database (delete example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Delete group for US
WITH CTE_DEPT(Name, GroupName)
AS
(
      SELECT Name, GroupName
      FROM [STAGE].[Department]
      WHERE GroupName = 'US Quality Assurance'
)
DELETE FROM CTE_DEPT;
GO

-- Show the data
SELECT * FROM [STAGE].[Department] 
GO


--
-- Investigating tempdb
--

-- Show time & i/o --
SET STATISTICS TIME ON
SET STATISTICS IO ON
GO

-- Do not show time & i/o --
SET STATISTICS TIME OFF
SET STATISTICS IO OFF
GO

-- Clean buffers & clear plan cache --
CHECKPOINT
DBCC DROPCLEANBUFFERS
DBCC FREEPROCCACHE
GO

-- What is executing ??
sp_who2

-- Pause for x seconds --
WAITFOR DELAY '00:00:30';

-- Check for new objects --
SELECT * FROM tempdb.sys.objects 
WHERE is_ms_shipped = 0

-- Space used by work table ??
SELECT * FROM sys.dm_db_session_space_usage
WHERE session_id = @@spid;


-- Actual start of data pages -- 
--   Reverse bytes 
--   First 4 bytes = file #
--   Last 4 bytes = page #

SELECT  T.name,
        T.[object_id],
        AU.type_desc,
        AU.first_page,
        AU.data_pages,
        P.[rows]
FROM    tempdb.sys.tables T
JOIN    tempdb.sys.partitions P
        ON  P.[object_id] = T.[object_id]
JOIN    tempdb.sys.system_internals_allocation_units AU
        ON  (AU.type_desc = N'IN_ROW_DATA' AND AU.container_id = P.partition_id)
        OR  (AU.type_desc = N'ROW_OVERFLOW_DATA' AND AU.container_id = P.partition_id)
        OR  (AU.type_desc = N'LOB_DATA' AND AU.container_id = P.hobt_id)
WHERE   T.name LIKE N'#A%';

-- Dump the data page --
DBCC TRACEON (3604);
DBCC PAGE (tempdb, 1, 296, 2) WITH TABLERESULTS;