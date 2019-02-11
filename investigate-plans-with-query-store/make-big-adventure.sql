/*
This script creates two new tables in AdventureWorks:

dbo.bigProduct
dbo.bigTransactionHistory
*/


USE AdventureWorks2014
GO

SELECT
	p.ProductID + (a.number * 1000) AS ProductID,
	p.Name + CONVERT(VARCHAR, (a.number * 1000)) AS Name,
	p.ProductNumber + '-' + CONVERT(VARCHAR, (a.number * 1000)) AS ProductNumber,
	p.MakeFlag,
	p.FinishedGoodsFlag,
	p.Color,
	p.SafetyStockLevel,
	p.ReorderPoint,
	p.StandardCost,
	p.ListPrice,
	p.Size,
	p.SizeUnitMeasureCode,
	p.WeightUnitMeasureCode,
	p.Weight,
	p.DaysToManufacture,
	p.ProductLine,
	p.Class,
	p.Style,
	p.ProductSubcategoryID,
	p.ProductModelID,
	p.SellStartDate,
	p.SellEndDate,
	p.DiscontinuedDate
INTO bigProduct
FROM Production.Product AS p
CROSS JOIN master..spt_values AS a
WHERE
	a.type = 'p'
	AND a.number BETWEEN 1 AND 50
GO


ALTER TABLE bigProduct
ALTER COLUMN ProductId INT NOT NULL	
GO

ALTER TABLE bigProduct
ADD CONSTRAINT pk_bigProduct PRIMARY KEY (ProductId)
GO


SELECT 
	ROW_NUMBER() OVER 
	(
		ORDER BY 
			x.TransactionDate,
			(SELECT NEWID())
	) AS TransactionID,
	p1.ProductID,
	x.TransactionDate,
	x.Quantity,
	CONVERT(MONEY, p1.ListPrice * x.Quantity * RAND(CHECKSUM(NEWID())) * 2) AS ActualCost
INTO bigTransactionHistory
FROM
(
	SELECT
		p.ProductID, 
		p.ListPrice,
		CASE
			WHEN p.productid % 26 = 0 THEN 26
			WHEN p.productid % 25 = 0 THEN 25
			WHEN p.productid % 24 = 0 THEN 24
			WHEN p.productid % 23 = 0 THEN 23
			WHEN p.productid % 22 = 0 THEN 22
			WHEN p.productid % 21 = 0 THEN 21
			WHEN p.productid % 20 = 0 THEN 20
			WHEN p.productid % 19 = 0 THEN 19
			WHEN p.productid % 18 = 0 THEN 18
			WHEN p.productid % 17 = 0 THEN 17
			WHEN p.productid % 16 = 0 THEN 16
			WHEN p.productid % 15 = 0 THEN 15
			WHEN p.productid % 14 = 0 THEN 14
			WHEN p.productid % 13 = 0 THEN 13
			WHEN p.productid % 12 = 0 THEN 12
			WHEN p.productid % 11 = 0 THEN 11
			WHEN p.productid % 10 = 0 THEN 10
			WHEN p.productid % 9 = 0 THEN 9
			WHEN p.productid % 8 = 0 THEN 8
			WHEN p.productid % 7 = 0 THEN 7
			WHEN p.productid % 6 = 0 THEN 6
			WHEN p.productid % 5 = 0 THEN 5
			WHEN p.productid % 4 = 0 THEN 4
			WHEN p.productid % 3 = 0 THEN 3
			WHEN p.productid % 2 = 0 THEN 2
			ELSE 1 
		END AS ProductGroup
	FROM bigproduct p
) AS p1
CROSS APPLY
(
	SELECT
		transactionDate,
		CONVERT(INT, (RAND(CHECKSUM(NEWID())) * 100) + 1) AS Quantity
	FROM
	(
		SELECT 
			DATEADD(dd, number, '20050101') AS transactionDate,
			NTILE(p1.ProductGroup) OVER 
			(
				ORDER BY number
			) AS groupRange
		FROM master..spt_values
		WHERE 
			type = 'p'
	) AS z
	WHERE
		z.groupRange % 2 = 1
) AS x



ALTER TABLE bigTransactionHistory
ALTER COLUMN TransactionID INT NOT NULL
GO


ALTER TABLE bigTransactionHistory
ADD CONSTRAINT pk_bigTransactionHistory PRIMARY KEY (TransactionID)
GO


CREATE NONCLUSTERED INDEX IX_ProductId_TransactionDate
ON bigTransactionHistory
(
	ProductId,
	TransactionDate
)
INCLUDE 
(
	Quantity,
	ActualCost
)
GO


