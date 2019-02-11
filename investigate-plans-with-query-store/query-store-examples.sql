-- Use Adventure Works
USE [AdventureWorks2014]
GO

-- Turn on query store (default values)
ALTER DATABASE AdventureWorks2014   
SET QUERY_STORE (  
    OPERATION_MODE = READ_WRITE,  
    CLEANUP_POLICY =   
    (STALE_QUERY_THRESHOLD_DAYS = 7),  
    DATA_FLUSH_INTERVAL_SECONDS = 900,  
    MAX_STORAGE_SIZE_MB = 100,  
    INTERVAL_LENGTH_MINUTES = 60,  
	SIZE_BASED_CLEANUP_MODE = AUTO,  
    QUERY_CAPTURE_MODE = AUTO,  
    MAX_PLANS_PER_QUERY = 200  
);  
GO


--
--  Same table but two plans
--

-- http://craftydba.com/?p=3767

-- Break down of orders per year using group by - (4 rows)
DECLARE @CNT INT = 2011;
WHILE (@CNT < 2015)
BEGIN

    -- Group by query (dynamic code)
	SELECT 
		YEAR(DueDate) as 'Year'
	  , COUNT(*) as 'Total'
	FROM
		[Sales].[SalesOrderHeader]
	WHERE 
	    YEAR(DueDate) = @CNT
	GROUP BY
		YEAR(DueDate)
	ORDER BY 
		YEAR(DueDate);

	-- Group by query (static code)
	SELECT 
		YEAR(DueDate) as 'Year'
	  , COUNT(*) as 'Total'
	FROM
		[Sales].[SalesOrderHeader]
	GROUP BY
		YEAR(DueDate)
	ORDER BY 
		YEAR(DueDate);

    -- Increment count
	SET @CNT = @CNT + 1;
	PRINT @CNT;

	-- Pause 1 minute
	--WAITFOR DELAY '00:01'
END
GO

-- Show Top Resource Consuming Queries

-- Clear the query store
ALTER DATABASE AdventureWorks2014 SET QUERY_STORE CLEAR; 
GO



--
--  Complicated multi-line table value function
--

-- Use the built in TVF for Adventure Works
SELECT * FROM [dbo].[ufnGetContactInformation] (1209);
GO
 
-- Get orders by country, customer, # orders, $ orders - (3582 rows)
SELECT
    ST.Name as Country,
    CI.LastName,
    CI.FirstName,
    COUNT(SH.SalesOrderID) as TotalOrders,
    SUM(SH.TotalDue) as TotalDue
FROM 
    [Sales].[SalesOrderHeader] SH 
    INNER JOIN [Sales].[Customer] AS C
	ON  SH.CustomerID = C.CustomerID
    INNER JOIN [Sales].[SalesTerritory] AS ST
	ON C.TerritoryID = ST.TerritoryID 
    CROSS APPLY [dbo].[ufnGetContactInformation] (C.PersonID) AS CI
WHERE
    C.StoreID IS NULL and ST.Name = 'Australia'
GROUP BY
    ST.Name,
    CI.LastName,
    CI.FirstName;
GO


-- Show plans in query store
SELECT  CAST(qsp.query_plan AS XML),
        qsq.query_id,
        qsp.plan_id,
        qsp.is_forced_plan
FROM    sys.query_store_query AS qsq
JOIN    sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id;
GO


--
--  Parameter sniffing example
--

-- http://www.scarydba.com/2016/05/16/query-store-forced-plans-new-plans/


-- Clear the query store
ALTER DATABASE AdventureWorks2014 SET QUERY_STORE CLEAR; 
GO

-- Create sp for parameter sniffing
CREATE PROC dbo.spAddressByCity @City NVARCHAR(30)
AS
SELECT  a.AddressID,
        a.AddressLine1,
        a.AddressLine2,
        a.City,
        sp.Name AS StateProvinceName,
        a.PostalCode
FROM    Person.Address AS a
JOIN    Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
WHERE   a.City = @City;
GO

-- Remove clean buffers & clear plan cache
CHECKPOINT 
DBCC DROPCLEANBUFFERS 
DBCC FREEPROCCACHE
GO

-- Plan A - Merge Join (include actual query plan)
exec dbo.spAddressByCity @City = 'London';
GO

-- Remove clean buffers & clear plan cache
CHECKPOINT 
DBCC DROPCLEANBUFFERS 
DBCC FREEPROCCACHE
GO

-- Plan B - Nested Loop (include actual query plan)
exec dbo.spAddressByCity @City = 'Mentor';
GO

-- Show plans in query store (collects data every 1 min)
SELECT  CAST(qsp.query_plan AS XML),
        qsq.query_id,
        qsp.plan_id,
        qsp.is_forced_plan
FROM    sys.query_store_query AS qsq
JOIN    sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
WHERE   qsq.object_id = OBJECT_ID('dbo.spAddressByCity');
go


-- 
--  Be careful with forced plans!
--

-- Remove plan (merge)
EXEC sys.sp_query_store_remove_plan @plan_id = 2;
GO

-- Force plan (hash)
EXEC sys.sp_query_store_force_plan @query_id = 4, @plan_id = 3;
go


-- Show forced plans in query store
SELECT  CAST(qsp.query_plan AS XML),
        qsq.query_id,
        qsp.plan_id,
        qsp.is_forced_plan
FROM    sys.query_store_query AS qsq
JOIN    sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
WHERE
	qsp.is_forced_plan = 1;
go

-- Unforce the plan (hash)
EXEC sys.sp_query_store_unforce_plan @query_id = 4, @plan_id = 3;
go



-- 
--  Live query statistics (button next 2 include actual plan)
--

/*
(25200 row(s) affected)
(31,263,601 row(s) affected)
*/

-- Clear the query store
ALTER DATABASE AdventureWorks2014 SET QUERY_STORE CLEAR; 
GO

-- Turn on sets options for this to work
SET STATISTICS XML ON; 
SET STATISTICS PROFILE ON;

-- Use big adventure tables
SELECT 
    FORMAT([TransactionDate],'yyyy-MM') as TransactionMonth,
	Color,
    SUM(Quantity) as Quantity,
	SUM(ActualCost) as ActualCost
FROM 
    [dbo].[bigTransactionHistory] t
    JOIN [dbo].[bigProduct]	p ON t.ProductID = p.ProductID
GROUP BY 
    FORMAT([TransactionDate],'yyyy-MM'), 
	Color;



-- Turn on query store
ALTER DATABASE AdventureWorks2014 SET QUERY_STORE = OFF; 
GO
