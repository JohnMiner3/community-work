-- Temp Table
DROP TABLE IF EXISTS [tmp].[FactInternetSales] 
GO

-- Grab last record
SELECT * 
INTO [tmp].[FactInternetSales] 
FROM [dbo].[FactInternetSales] 
WHERE SalesOrderNumber = 'SO75123';
GO

-- Make new record
UPDATE 
	[tmp].[FactInternetSales] 
SET 
	OrderDateKey = 20140704,
    DueDateKey = 20140704,
	ShipDateKey = 20140704,
	SalesOrderNumber = 'SO80000';

-- Add record to table
INSERT INTO [dbo].[FactInternetSales] 
SELECT * FROM [tmp].[FactInternetSales];

-- Remove record from table
--DELETE FROM [dbo].[FactInternetSales]  WHERE SalesOrderNumber = 'SO80000';

SELECT MAX(SalesOrderNumber) FROM [dbo].[FactInternetSales] 


