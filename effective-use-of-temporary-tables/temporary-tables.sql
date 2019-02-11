/******************************************************
 *
 * Name:         temporary-tables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     10-09-2012
 *     Purpose:  Samples of temporary tables.
 * 
 ******************************************************/


--
-- Auto database (create table)
--

-- Database selection does not matter
USE [model];
GO

-- Create local temp table
CREATE TABLE #A
(
  MyId INT PRIMARY KEY CLUSTERED,
  MyValue VARCHAR(20)
)
GO


--
-- Auto database (insert data)
--

-- Add data to local temp table
INSERT INTO #A (MyId, MyValue) VALUES
  (1, 'Continental'),
  (2, 'Edsel'),
  (3, 'Lincoln'),
  (4, 'Mercury'),
  (5, 'Ram')
GO

-- Show the data
SELECT * FROM #A;
GO


--
-- Auto database (modify table - ddl)
--

-- Add a new column
ALTER TABLE #A ADD [ModifiedDate] smalldatetime NULL;

-- Add a new constraint
ALTER TABLE #A ADD CONSTRAINT df_Modified_Date
  DEFAULT GETDATE() FOR [ModifiedDate];
GO


--
-- Auto database (modify data - dml)
--

-- Show data from table A
SELECT MyId, MyValue
FROM #A
GO

-- Remove record 3
DELETE FROM #A WHERE MyId = 3;
GO

-- Update record 5
UPDATE #A
SET MyValue = 'Dodge Ram'
WHERE MyId = 5;
GO

-- Show up dated data from table A
SELECT MyId, MyValue
FROM #A
GO


--
-- Auto database (name conflict)
--

-- Create local temp table
CREATE TABLE #B
(
  MyId INT PRIMARY KEY CLUSTERED,
  MyValue VARCHAR(20)
)
GO

-- Add data to local temp table
INSERT INTO #B (MyId, MyValue) VALUES
  (1, 'Continental'),
  (2, 'Edsel'),
  (3, 'Lincoln'),
  (4, 'Mercury'),
  (5, 'Ram')
GO

-- Show the data
SELECT * FROM #B;
GO

-- Add a new column
ALTER TABLE #B ADD [ModifiedDate] smalldatetime NULL;

-- Add a new constraint
ALTER TABLE #B ADD CONSTRAINT df_Modified_Date
  DEFAULT GETDATE() FOR [ModifiedDate];
GO


--
-- Auto database (drop table)
--

-- Table names are stored with system post fix at end
IF EXISTS (SELECT * FROM tempdb.sys.objects T WHERE T.TYPE = 'U' AND T.name LIKE '#A%')
  DROP TABLE #A
GO


--
-- AW database (create table)
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Local temp table (MC) - last sales order by customer in 2008
SELECT
  CustomerID AS MyCustomer,
  MAX(OrderDate) AS MyDate
INTO
  #MC
FROM
  Sales.SalesOrderHeader
WHERE
  YEAR(OrderDate) = 2008
GROUP BY
  CustomerID
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
  INNER JOIN #MC AS MC ON SH.CustomerID = MC.MyCustomer AND SH.OrderDate = MC.MyDate
  INNER JOIN Sales.Customer C ON MC.MyCustomer = C.CustomerID
  INNER JOIN Person.Person P ON C.PersonID = P.BusinessEntityID
ORDER BY
  SH.OrderDate ASC;
GO


--
-- AW database (drop table)
--

-- Table names are stored with system post fix at end
IF EXISTS (SELECT * FROM tempdb.sys.objects T WHERE T.TYPE = 'U' AND T.name LIKE '#MC%')
  DROP TABLE #MC
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
DBCC PAGE (tempdb, 1, 296, 3) WITH TABLERESULTS;