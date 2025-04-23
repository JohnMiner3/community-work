--
--  S.O.H. - Code for data pipeline source
--

-- Ignore counts
SET NOCOUNT ON;

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



--
--  S.O.D. - Code for data pipeline source
--

-- Ignore counts
SET NOCOUNT ON;

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