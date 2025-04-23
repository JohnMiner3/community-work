--
--  1 - Find recent records
--

select * from [SalesLT].[SalesOrderHeader] where [SalesOrderID] = 80005
select * from [SalesLT].[SalesOrderDetail] where [SalesOrderID] = 80005
GO


--
--  2 - Add header record
--

-- Allow inserts
SET IDENTITY_INSERT [AdventureWorksLT2012].[SalesLT].[SalesOrderHeader] ON
GO

INSERT INTO [AdventureWorksLT2012].[SalesLT].[SalesOrderHeader]
(
    [SalesOrderID]
	,[RevisionNumber]
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
    ,[ModifiedDate]
)
SELECT 80005 AS [SalesOrderID]
      ,[RevisionNumber]
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
      , NEWID()
      ,[ModifiedDate]
FROM [AdventureWorksLT2012].[SalesLT].[SalesOrderHeader]
WHERE [SalesOrderID] = 71948
GO

-- Don't allow inserts
SET IDENTITY_INSERT [AdventureWorksLT2012].[SalesLT].[SalesOrderHeader] OFF
GO


--
--  3 - Add detail record
--

-- Allow inserts
SET IDENTITY_INSERT [AdventureWorksLT2012].[SalesLT].[SalesOrderDetail] ON 
GO

INSERT INTO [AdventureWorksLT2012].[SalesLT].[SalesOrderDetail]
(
	[SalesOrderID]
	,[SalesOrderDetailID]
    ,[OrderQty]
    ,[ProductID]
    ,[UnitPrice]
    ,[UnitPriceDiscount]
    ,[rowguid]
    ,[ModifiedDate]
)
SELECT 80005 as [SalesOrderID]
      , 180005 AS [SalesOrderDetailID]
      ,[OrderQty]
      ,[ProductID]
      ,[UnitPrice]
      ,[UnitPriceDiscount]
      ,NEWID()
      ,[ModifiedDate]
FROM [AdventureWorksLT2012].[SalesLT].[SalesOrderDetail]
WHERE [SalesOrderID] = 71948
GO

-- Don't allow inserts
SET IDENTITY_INSERT [AdventureWorksLT2012].[SalesLT].[SalesOrderDetail] OFF
GO

