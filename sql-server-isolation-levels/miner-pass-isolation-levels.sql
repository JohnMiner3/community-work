/******************************************************
 *
 * Name:         big-johns-bbq-database-sharding.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Convert the Big Jons BBQ data warehouse
 *               into a sharding example.
 *
 ******************************************************/


-- Switch to correct database
Use [AdventureWorks2012];
GO



--
-- EX01 - System Transactions (spid < 50)
--

select 
    min(spid) as system_min_spid, 
    cmd as system_transactions, 
    lastwaittype as system_wait_type, 
    count(spid) as system_threads
from 
    sys.sysprocesses where spid < 50
group by 
    cmd, lastwaittype 
order by 
    min(spid);
GO



--
-- EX02 - System Transactions (ghost cleanup)
--

-- Drop existing temp table
IF EXISTS (SELECT * FROM tempdb.sys.objects T WHERE T.TYPE = 'U' AND T.name LIKE '#tmp_requests%')
  DROP TABLE #tmp_requests
GO

-- Create new temp table
SELECT * 
INTO #tmp_requests 
FROM sys.dm_exec_requests 
WHERE 1 = 0;
GO

-- Declare local variables
DECLARE @var_cnt1 INT = 0;

-- Look for the ghost thread
WHILE (@var_cnt1 < 1)
BEGIN

    -- Is the process running? 
    INSERT INTO #tmp_requests 
    SELECT * 
    FROM sys.dm_exec_requests 
    WHERE command LIKE '%ghost%';

	-- Count = 0, then continue
    SELECT @var_cnt1 = COUNT(*) FROM #tmp_requests

END;
GO

-- Show the result
SELECT * FROM #tmp_requests;
GO



--
-- EX03 - User Transactions
--

/* 
    Session #1 - Page data
*/

-- Declare local variables
DECLARE @var_remains INT = 1;
DECLARE @var_offset INT = 0;
DECLARE @var_rows INT = 25;

-- Traverse all the sales orders
WHILE (@var_remains > 0)
BEGIN

    -- Page by 25 rows at a time
    SELECT [SalesOrderID], [OrderDate], [CustomerID], [SalesPersonID]
    FROM Sales.SalesOrderHeader
    ORDER BY [OrderDate] DESC, [SalesOrderID] DESC
    OFFSET @var_offset ROWS 
	FETCH NEXT @var_rows ROWS ONLY;

    -- Update counters
    SELECT 
        @var_offset += 25,
        @var_remains = COUNT(*) - @var_offset
    FROM Sales.SalesOrderHeader;

	-- Debugging lines
    PRINT 'OFFSET = ' + STR(@var_offset, 6, 0);
    PRINT 'REMAINDER = ' + STR(@var_remains, 6, 0);
    PRINT '';

END;
GO


/* 
    Session #2 - Active transactions
*/

-- Open another window
sp_who2
GO

-- Look at work tables
SELECT * 
FROM sys.dm_tran_active_transactions;
GO



--
-- EX04 - Auto-commit transaction mode (default)
--

-- Show the before/after data
SELECT SalesOrderID, Comment
FROM Sales.SalesOrderHeader SOH
WHERE SOH.SalesOrderID = 43659;
GO 

-- Update to a nifty comment
UPDATE SOH
SET Comment = 'Learn about transactions is fun!'
FROM Sales.SalesOrderHeader SOH
WHERE SOH.SalesOrderID = 43659;
GO 



--
-- EX05 - Implicit transaction mode
--

-- Turn it on
SET IMPLICIT_TRANSACTIONS ON;
GO

-- Update to a nifty comment
UPDATE SOH
SET Comment = 'add something to say here ...'
FROM Sales.SalesOrderHeader SOH
WHERE SOH.SalesOrderID = 43659;

-- Middle of a transaction
SELECT 
    CASE XACT_STATE()
	    WHEN 1 THEN 'active user transaction'
	    WHEN 0 THEN 'no active user transaction'
	    WHEN -1 THEN 'uncommittable transaction'
	    ELSE 'unexpected code'
	END	AS transaction_state;

-- Show the before/after data
SELECT SalesOrderID, Comment
FROM Sales.SalesOrderHeader SOH
WHERE SOH.SalesOrderID = 43659;

-- Undo the change
ROLLBACK TRAN;

-- Turn it off
SET IMPLICIT_TRANSACTIONS OFF;
GO



--
-- EX06 - Explicit transaction mode
--

-- Show the before/after data
SELECT SalesOrderID, Comment
FROM Sales.SalesOrderHeader SOH
WHERE SOH.SalesOrderID = 43659;


-- Use explicit transactions
BEGIN TRAN PASS2013

    -- Clear out the comment
    UPDATE SOH
    SET Comment = ''
    FROM Sales.SalesOrderHeader SOH
    WHERE SOH.SalesOrderID = 43659;

	-- Show open transactions
	DBCC OPENTRAN('AdventureWorks2012');

-- Commit the named transaction
COMMIT TRAN PASS2013;
GO



--
-- EX07 - Exclusive row lock
--

-- Begin a trans
BEGIN TRAN 

    -- Hold the lock
    SELECT *
    FROM Sales.SalesOrderHeader AS SOH WITH (HOLDLOCK, ROWLOCK, XLOCK)
    WHERE SOH.SalesOrderID = 43659;

-- Roll back
ROLLBACK;



--
-- EX08 - Shared table lock
--

-- Begin a trans
BEGIN TRAN 

    -- Hold the lock
    SELECT *
    FROM Sales.SalesOrderHeader AS SOH WITH (HOLDLOCK, TABLOCK)
    WHERE SOH.SalesOrderID = 43659;

-- Roll back
ROLLBACK;


--
-- Locked object details
-- 

-- Old school technique
EXEC sp_lock
GO

-- Lock details
SELECT
    resource_type, resource_associated_entity_id,
    request_status, request_mode,request_session_id,
    resource_description 
    FROM sys.dm_tran_locks
    WHERE resource_database_id = DB_ID('AdventureWorks2012')
GO

-- Page/Key details
SELECT object_name(object_id) as object_nm, *
    FROM sys.partitions
    WHERE hobt_id = 72057594047037440
GO

-- Object details
SELECT object_name(1266103551)
GO



--
-- Run in another window (works for S and blocks for X)
-- 

-- Additional select stmt (X)
SELECT *
FROM Sales.SalesOrderHeader AS SOH WITH (HOLDLOCK, ROWLOCK, XLOCK)
WHERE SOH.SalesOrderID = 43659;


-- Additional select stmt (S)
SELECT *
FROM Sales.SalesOrderHeader AS SOH
WHERE SOH.SalesOrderID = 43659;


--
-- Blocking details (by hand)
-- 

SELECT 
    t1.resource_type,
    t1.resource_database_id,
    t1.resource_associated_entity_id,
    t1.request_mode,
    t1.request_session_id,
    t2.blocking_session_id
FROM 
    sys.dm_tran_locks as t1
INNER JOIN 
    sys.dm_os_waiting_tasks as t2
ON 
    t1.lock_owner_address = t2.resource_address;
GO


--
-- Clear local tables used by monitoring
--

-- Switch to correct database
Use [MSDB];
GO

-- Xml reports
DELETE FROM [dbo].[tbl_Wmi_Xml_Reports]
GO

-- Blocking table
DELETE FROM [dbo].[tbl_Monitor_Blocking]
GO

-- Deadlock table
DELETE FROM [dbo].[tbl_Monitor_Deadlocks]
GO


--
-- Service Broker must be enabled
-- 

-- Broker enabled flag by database
SELECT 
  name, 
  is_broker_enabled, 
  service_broker_guid 
FROM sys.databases;

-- Turn on adventure works
ALTER DATABASE AdventureWorks2012 
  SET ENABLE_BROKER;



--
-- EX09 - An example of blocking
--


/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Window 1 (spid x)
EXEC usp_Blocking_Test_Thread1 21;
GO


/* 
    Session #2
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Window 2 (spid y)
EXEC usp_Blocking_Test_Thread2 21;
GO


--
-- Look at WMI XML table
--

SELECT * FROM msdb.dbo.tbl_Wmi_Xml_Reports ORDER BY alert_id desc
GO


--
-- Look at email queue
--

SELECT * FROM msdb.dbo.sysmail_mailitems ORDER BY send_request_date DESC
GO



--
-- EX10 - An example of blocking then deadlock
--

-- Save information to SQL log
DBCC TRACEON (1204, 1205, 3605, -1)
GO

-- Save information to SQL log
DBCC TRACEOFF (1204, 1205, 3605, -1)
GO


/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Window 1 (spid x)
EXEC usp_Deadlock_Test_Thread1;
GO


/* 
    Session #2
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Window 2 (spid y)
EXEC usp_Deadlock_Test_Thread2;
GO


--
-- EX11 - An example of read uncommitted isolation level (dirty read)
--

/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Update that is rolled back
BEGIN TRAN

	-- Update the resource (writer)
	UPDATE [Person].[Person]
	SET Title = 'Mr.'
	WHERE BusinessEntityID = 7;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

-- Roll back the change
ROLLBACK;


/* 
    Session #2
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Allow dirty reads
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
GO

-- Select top 10 records
SELECT TOP 10 * 
FROM [Person].[Person]
ORDER BY BusinessEntityID;



--
-- PRE12 - Setup a table w/o a identity column
--

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'Temp')
DROP SCHEMA [Temp]
GO

-- Add new schema.
CREATE SCHEMA [Temp] AUTHORIZATION [dbo]
GO

-- Delete existing table
IF  OBJECT_ID(N'[Temp].[ContactType]') > 0
    DROP TABLE [Temp].[ContactType]
GO

-- Create new table
CREATE TABLE [Temp].[ContactType]
(
	[ContactTypeID] [int] NOT NULL PRIMARY KEY CLUSTERED,
	[Name] [dbo].[Name] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL DEFAULT (getdate()) 
) 
GO

-- Move data to new table
INSERT INTO [Temp].[ContactType]
  SELECT * FROM [Person].[ContactType]
GO

-- Show the data
SELECT * FROM [Temp].[ContactType]
GO



--
-- EX12 - An example of repeatable read isolation level (moved row)
--

/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Update that is committed
BEGIN TRAN

	-- Lock the record
	UPDATE [Temp].[ContactType]
	SET ContactTypeId = 7
	WHERE ContactTypeId = 7;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

	-- Move a unscanned record
	UPDATE [Temp].[ContactType]
	SET ContactTypeId = -1
	WHERE ContactTypeId = 8;

-- Commit the change
COMMIT;


/* 
    Session #2
*/

-- Session level change
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
GO

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Read the data, PK is id
SELECT *
FROM [Temp].[ContactType];
GO



--
-- EX13 - An example of repeatable read isolation level (phantom read)
--

/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Session level change
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
GO

-- Start a transaction
BEGIN TRAN

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

-- Commit the transaction
COMMIT;


/* 
    Session #2
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Update so that record is in range
UPDATE [Temp].[ContactType]
SET ContactTypeId = 8
WHERE ContactTypeId = -1;



--
-- EX14 - An example of read committed snapshot isolation level 
--

-- Database level, all trans must finish
ALTER DATABASE [AdventureWorks2012] 
SET READ_COMMITTED_SNAPSHOT ON;
GO

-- Current session
SELECT 
  CASE transaction_isolation_level 
    WHEN 0 THEN 'Unspecified' 
    WHEN 1 THEN 'ReadUncommitted' 
    WHEN 2 THEN 'ReadCommitted' 
    WHEN 3 THEN 'Repeatable' 
    WHEN 4 THEN 'Serializable' 
    WHEN 5 THEN 'Snapshot' 
  END AS TRANSACTION_ISOLATION_LEVEL 
FROM sys.dm_exec_sessions 
WHERE session_id = @@SPID;
GO


/* 
    Session #1
*/


-- Start a transaction
BEGIN TRAN

    -- Update id
	UPDATE [Temp].[ContactType]
	SET ContactTypeId = -1
	WHERE ContactTypeId = 8;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

-- Rollback the transaction
ROLLBACK;


/* 
    Session #2 (reads no longer blocked)
*/


-- Read the data, not blocking
SELECT *
FROM [Temp].[ContactType];
GO

-- What are we using in the store?
SELECT * 
FROM sys.dm_tran_top_version_generators
GO


/*
    Turn off read committed
*/
 
-- Database level, all trans must finish
ALTER DATABASE [AdventureWorks2012] 
SET READ_COMMITTED_SNAPSHOT OFF;
GO



--
-- EX15 - An example of snapshot isolation level 
--


-- Database level, all trans must finish
ALTER DATABASE [AdventureWorks2012] 
SET ALLOW_SNAPSHOT_ISOLATION  ON;
GO


/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Session level change
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
GO

-- Start a transaction
BEGIN TRAN

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

-- Commit the transaction
COMMIT;

-- What are we using in the store?
SELECT * 
FROM sys.dm_tran_top_version_generators
GO


/* 
    Session #2
*/

-- Start a transaction
BEGIN TRAN

    -- Update id
	UPDATE [Temp].[ContactType]
	SET ContactTypeId = -ContactTypeId;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

-- Rollback the transaction
ROLLBACK;


/*
    Turn off snapshot isolation
*/
 
-- Database level, all trans must finish
ALTER DATABASE [AdventureWorks2012] 
SET ALLOW_SNAPSHOT_ISOLATION OFF;
GO



--
-- EX16 - An example of sesrializable isolation level 
--

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- What does our dataset look like?
SELECT * FROM [Temp].[ContactType]
GO


/* 
    Session #1
*/

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Session level change
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
GO

-- Start a transaction
BEGIN TRAN

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

	-- Wait for 20 seconds
    WAITFOR DELAY '00:00:20';

    -- Read the data, PK is id
    SELECT *
    FROM [Temp].[ContactType]
	WHERE ContactTypeId > 0;

-- Commit the transaction
COMMIT;


/* 
    Session #2
*/

-- Update id, take row out of select range
UPDATE [Temp].[ContactType]
SET ContactTypeId = -1
WHERE ContactTypeId = 8;
GO

/* 
    Session #3
*/

-- Update id, put row in select range
UPDATE [Temp].[ContactType]
SET ContactTypeId = 8
WHERE ContactTypeId = -1;
GO



--
-- EX17 - Dirty Reads & Page Splits
--

-- Switch to correct database
Use [AdventureWorks2012];
GO

-- Delete existing table
IF  OBJECT_ID(N'[Temp].[PageSplits]') > 0
    DROP TABLE [Temp].[PageSplits]
GO

-- Create new table
CREATE TABLE [Temp].[PageSplits]
(
	[SplitId] [int] IDENTITY (1, 1) NOT NULL,
	[SplitGuid] UNIQUEIDENTIFIER NOT NULL DEFAULT (NEWSEQUENTIALID()),
	[SplitDt] [datetime] NOT NULL DEFAULT (getdate()),
    CONSTRAINT [pk_Split_Guid] PRIMARY KEY CLUSTERED 
    ( [SplitGuid] ASC )
) 
GO

-- Make 25K of records
DECLARE @VAR_CNT INT = 1;
WHILE (@VAR_CNT <= 2500)
BEGIN
    INSERT [Temp].[PageSplits] DEFAULT VALUES;
	SET @VAR_CNT = @VAR_CNT + 1;
END
GO

-- Get record count
SELECT COUNT(*) AS TOTAL_RECS 
FROM [Temp].[PageSplits]
GO


/*

~~ Space used ~~
name        rows    reserved    data    index   unused
PageSplits	2500   	136 KB	    96 KB	16 KB	24 KB

*/

sp_spaceused '[Temp].[PageSplits]'
GO



/* 
    Session #1 - Move the records around
*/


-- Change the guid/date
WHILE (1 <> 2)
BEGIN
    UPDATE 
	    [Temp].[PageSplits] 
	SET 
	    [SplitGuid] = NEWID(), 
	    [SplitDt] = GETDATE();
END
GO


/* 
    Session #2 - Get record counts
*/

-- Count the records
DECLARE @CNT INT = 0;
WHILE (1 <> 2)
BEGIN

    -- Just select data
    SELECT @CNT = COUNT(*) 
    FROM [Temp].[PageSplits] WITH (NOLOCK);
--  FROM [Temp].[PageSplits] WITH (READUNCOMMITTED);

    -- Print the count
    PRINT 'CNT:  ' + STR(@CNT, 4, 0);

	-- Wait for 1/4 second
    WAITFOR DELAY '00:00:00.250';

END
GO
