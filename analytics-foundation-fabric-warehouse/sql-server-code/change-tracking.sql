--
--  1 - Enable change tracking at database level
--

-- switch database
USE [master];
GO

-- turn on tracking
ALTER DATABASE AdventureWorksLT2012  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
GO

--
--  2 - Enable change tracking at table level
--

-- switch database
USE [AdventureWorksLT2012];
GO

-- alter table 1
ALTER TABLE SalesLT.SalesOrderDetail  
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- alter table 2
ALTER TABLE SalesLT.SalesOrderHeader
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);
GO



--
--  3 - Validate that table tracking is on
--

SELECT 
    s.name as schema_nm, 
    t.name as table_nm, 
    c.is_track_columns_updated_on as ct_enabled
FROM 
    sys.schemas as s 
JOIN 
    sys.tables as t
ON 
    s.schema_id = t.schema_id
JOIN 
    sys.change_tracking_tables AS c
ON 
    t.object_id = c.object_id;
GO


--
--  4 - Validate that database tracking is on
--

SELECT * FROM sys.change_tracking_databases;
GO


--
--  5 - Disable change tracking at table level
--

-- switch database
USE [AdventureWorksLT2012];
GO

-- alter table 1
ALTER TABLE SalesLT.SalesOrderDetail    
DISABLE CHANGE_TRACKING; 
GO

-- alter table 2
ALTER TABLE SalesLT.SalesOrderHeader
DISABLE CHANGE_TRACKING; 
GO



--
--  6 - Disable change tracking at database level
--

-- switch database
USE [master];
GO

-- turn off tracking
ALTER DATABASE AdventureWorksLT2012  
SET CHANGE_TRACKING = OFF;
GO



--
--  7 - Create + populate custom table
--

-- drop existing
DROP TABLE IF EXISTS dbo.TrackTableChanges;
GO

-- create table
CREATE TABLE dbo.TrackTableChanges
(
    id int IDENTITY(1, 1) NOT NULL,
    table_nm varchar(128),
    version_no bigint,
    event_dte datetime2(6)
);
GO

-- add data
INSERT INTO dbo.TrackTableChanges
(
    table_nm,
    version_no,
    event_dte
)
SELECT 
    'SalesLT.SalesOrderDetail',
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('SalesLt.SalesOrderDetail')),
    GETDATE()
UNION
SELECT 
    'SalesLT.SalesOrderHeader',
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('SalesLt.SalesOrderHeader')),
    GETDATE()
GO

-- show versions
SELECT * FROM dbo.TrackTableChanges
GO



--
--  S.O.D. - Code for data pipeline source
--

-- Ignore counts
SET NOCOUNT ON

-- Local variable
DECLARE @CT_OLD BIGINT;
DECLARE @CT_NEW BIGINT;


-- Saved tracking version (old)
SELECT TOP 1 @CT_OLD = [version_no]
FROM [dbo].[TrackTableChanges]
WHERE [table_nm] = 'SalesLT.SalesOrderDetail'
ORDER BY [event_dte] DESC;


-- Current tracking version (new)
SELECT @CT_NEW = CHANGE_TRACKING_CURRENT_VERSION();


-- Grab table changes
SELECT
    D.*,
    C.SYS_CHANGE_OPERATION, 
    C.SYS_CHANGE_VERSION
FROM 
    SalesLT.SalesOrderDetail AS D
RIGHT OUTER JOIN
    CHANGETABLE(CHANGES SalesLT.SalesOrderDetail, @CT_OLD) AS C
ON
    D.[SalesOrderID] = C.[SalesOrderID] AND
    D.[SalesOrderDetailID] = C.[SalesOrderDetailID]
WHERE 
    C.SYS_CHANGE_VERSION <= @CT_NEW;


-- Update local tracking table?
IF (@@ROWCOUNT > 0)
BEGIN
    INSERT INTO [dbo].[TrackTableChanges]
    (
        table_nm,
        version_no,
        event_dte
    )
    VALUES
    (
        'SalesLT.SalesOrderDetail',
        @CT_NEW,
	GETDATE()
    );
END





--
--  S.O.H. - Code for data pipeline source
--

-- Ignore counts
SET NOCOUNT ON

-- Local variable
DECLARE @CT_OLD BIGINT;
DECLARE @CT_NEW BIGINT;


-- Saved tracking version (old)
SELECT TOP 1 @CT_OLD = [version_no]
FROM [dbo].[TrackTableChanges]
WHERE [table_nm] = 'SalesLT.SalesOrderHeader'
ORDER BY [event_dte] DESC;


-- Current tracking version (new)
SELECT @CT_NEW = CHANGE_TRACKING_CURRENT_VERSION();


-- Grab table changes
SELECT
    D.*,
    C.SYS_CHANGE_OPERATION, 
    C.SYS_CHANGE_VERSION
FROM 
    SalesLT.SalesOrderHeader AS D
RIGHT OUTER JOIN
    CHANGETABLE(CHANGES SalesLT.SalesOrderHeader, @CT_OLD) AS C
ON
    D.[SalesOrderID] = C.[SalesOrderID]
WHERE 
    C.SYS_CHANGE_VERSION <= @CT_NEW;


-- Update local tracking table?
IF (@@ROWCOUNT > 0)
BEGIN
    INSERT INTO [dbo].[TrackTableChanges]
    (
        table_nm,
        version_no,
        event_dte
    )
    VALUES
    (
        'SalesLT.SalesOrderHeader',
        @CT_NEW,
	GETDATE()
    );
END


--
--  S.O.H. - Insert duplicate record
-- 

INSERT INTO SalesLT.SalesOrderHeader
(
  [RevisionNumber]
 ,[OrderDate]
 ,[DueDate]
 ,[ShipDate]
 ,[Status]
 ,[OnlineOrderFlag]
 ,[PurchaseOrderNumber]
 ,[AccountNumber]
 ,[CustomerID]
 ,[ShipToAddressID]
 ,[BillToAddressID]
 ,[ShipMethod]
 ,[CreditCardApprovalCode]
 ,[SubTotal]
 ,[TaxAmt]
 ,[Freight]
 ,[Comment]
 ,[rowguid]
 ,[ModifiedDate])
SELECT 
  [RevisionNumber]
 ,[OrderDate]
 ,[DueDate]
 ,[ShipDate]
 ,[Status]
 ,[OnlineOrderFlag]
 ,[PurchaseOrderNumber]
 ,[AccountNumber]
 ,[CustomerID]
 ,[ShipToAddressID]
 ,[BillToAddressID]
 ,[ShipMethod]
 ,[CreditCardApprovalCode]
 ,[SubTotal]
 ,[TaxAmt]
 ,[Freight]
 ,[Comment]
 ,NEWID()
 ,[ModifiedDate]
FROM
  SalesLT.SalesOrderHeader 
WHERE 
  SalesOrderId = 71946;
GO


--
--  Update key fields
--

UPDATE 
  SalesLT.SalesOrderHeader 
SET
  OrderDate = DATEADD(YEAR, 21, OrderDate),
  DueDate = DATEADD(YEAR, 21, DueDate),
  ShipDate = DATEADD(YEAR, 21, ShipDate),
  ModifiedDate = DATEADD(YEAR, 21, ModifiedDate),
  PurchaseOrderNumber = 'PO20250311123015'
WHERE 
  SalesOrderId = 71948;


--
--  Show final record
--

SELECT
  *
FROM
  SalesLT.SalesOrderHeader 
WHERE 
  SalesOrderId = 71948;
GO


--
--  S.O.D. - Insert duplicate record
-- 

INSERT INTO [SalesLT].[SalesOrderDetail]
(
    [SalesOrderID]
   ,[OrderQty]
   ,[ProductID]
   ,[UnitPrice]
   ,[UnitPriceDiscount]
   ,[rowguid]
   ,[ModifiedDate]
)
SELECT 
    71948
   ,[OrderQty]
   ,[ProductID]
   ,[UnitPrice]
   ,[UnitPriceDiscount]
   ,NEWID()
   ,DATEADD(YEAR, 21, ModifiedDate)
FROM
  SalesLT.SalesOrderDetail
WHERE 
  SalesOrderId = 71946;
GO


--
--  Show final record
--

SELECT
  *
FROM
  SalesLT.SalesOrderDetail 
WHERE 
  SalesOrderId = 71948;
GO

