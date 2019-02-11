/******************************************************
 *
 * Name:         derived-tables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     10-09-2012
 *     Purpose:  Samples of derived tables.
 * 
 ******************************************************/


--
-- Auto database (select example)
--

-- Database selection does not matter
USE [model];
GO

-- Create a derived table
SELECT * FROM
(
  VALUES
  (1, 'Continental'),
  (2, 'Edsel'),
  (3, 'Lincoln'),
  (4, 'Mercury'),
  (5, 'Ram')
) AS A (MyId, MyValue);
GO


--
-- AW database (select example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Get customer name, sales id, sub-total and order date
SELECT
  P.FirstName,
  P.LastName,
  SH.SalesOrderID,
  SH.SubTotal,
  SH.OrderDate
FROM
  Sales.SalesOrderHeader AS SH

  -- Derived table - last order by customer in 2008
  INNER JOIN
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
  ) AS MC

  ON SH.CustomerID = MC.MyCustomer AND SH.OrderDate = MC.MyDate
  INNER JOIN Sales.Customer C ON MC.MyCustomer = C.CustomerID
  INNER JOIN Person.Person P ON C.PersonID = P.BusinessEntityID
ORDER BY
  SH.OrderDate ASC;
GO

-- Pause for x seconds
WAITFOR DELAY '00:00:30';


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
UPDATE T1
SET GroupName = 'US Quality Assurance'
FROM
  (
      SELECT * FROM [STAGE].[Department]
      WHERE GroupName = 'Quality Assurance'
  ) AS T1;
GO


--
-- AW database (insert example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Insert group for ASIA
INSERT INTO [STAGE].[Department] (Name, GroupName)
SELECT T1.Name, 'ASIA ' + SUBSTRING(T1.GroupName, 4, LEN(T1.GroupName) - 3) AS GroupName
FROM
    (
      SELECT * FROM [STAGE].[Department]
      WHERE GroupName = 'US Quality Assurance'
    ) AS T1;
GO


--
-- AW database (delete example)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Delete group for US
DELETE
FROM T1
FROM
    (
      SELECT DepartmentID FROM [STAGE].[Department]
      WHERE GroupName = 'US Quality Assurance'
    ) AS T1;
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

-- Internal tables
select * from tempdb.sys.internal_tables
go

-- Space used by work table ??
SELECT * FROM sys.dm_db_session_space_usage
WHERE session_id = @@spid;

-- Show databases by id
select * from sys.databases

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
