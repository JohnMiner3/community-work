/******************************************************
 *
 * Name:         temporary-variables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     10-09-2012
 *     Purpose:  Samples of temporary variables.
 * 
 ******************************************************/


--
-- Auto database
--

-- Database selection does not matter
USE [model];
GO

-- Create local table variable
DECLARE @MY_TABLE TABLE
(
  MyId INT PRIMARY KEY CLUSTERED,
  MyValue VARCHAR(20)
);

-- Add data to local temp table
INSERT INTO @MY_TABLE (MyId, MyValue) VALUES
  (1, 'Continental'),
  (2, 'Edsel'),
  (3, 'Lincoln'),
  (4, 'Mercury'),
  (5, 'Ram');

-- Show data from table A
SELECT T.MyId, T.MyValue
FROM @MY_TABLE AS T

-- Pause for 30 seconds
WAITFOR DELAY '00:00:30';

-- Remove record 3
DELETE FROM @MY_TABLE
WHERE MyId = 3;

-- Show data from table A
SELECT T.MyId, T.MyValue
FROM @MY_TABLE AS T

-- Update record 5
UPDATE @MY_TABLE
SET MyValue = 'Dodge Ram'
WHERE MyId = 5;

-- Show up dated data from table A
SELECT MyId, MyValue
FROM @MY_TABLE
GO

-- Pause for 30 seconds
WAITFOR DELAY '00:00:30';


--
-- AW database 
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Create local table variable
DECLARE @MC TABLE
(
  MyCustomer INT,
  MyDate DATETIME
);

-- Local temp table (MC) - last sales order by customer in 2008
INSERT INTO @MC
SELECT
  CustomerID AS MyCustomer,
  MAX(OrderDate) AS MyDate
FROM
  Sales.SalesOrderHeader
WHERE
  YEAR(OrderDate) = 2008
GROUP BY
  CustomerID;

-- Get customer name, sales id, sub-total and order date
SELECT
  P.FirstName,
  P.LastName,
  SH.SalesOrderID,
  SH.SubTotal,
  SH.OrderDate
FROM
  Sales.SalesOrderHeader AS SH
  INNER JOIN @MC AS MC ON SH.CustomerID = MC.MyCustomer AND SH.OrderDate = MC.MyDate
  INNER JOIN Sales.Customer C ON MC.MyCustomer = C.CustomerID
  INNER JOIN Person.Person P ON C.PersonID = P.BusinessEntityID
ORDER BY
  SH.OrderDate ASC

-- Pause for a minute
WAITFOR DELAY '00:01';
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
