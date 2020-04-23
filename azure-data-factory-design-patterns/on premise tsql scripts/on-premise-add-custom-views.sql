USE [AdventureWorksDW]
GO
/****** Object:  View [dbo].[vGeography]    Script Date: 4/22/2020 9:31:15 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vGeography]
as
SELECT [GeographyKey]
      ,[City]
      ,[StateProvinceCode]
      ,[StateProvinceName]
      ,[CountryRegionCode]
      ,[EnglishCountryRegionName]
      ,[PostalCode]
      ,[SalesTerritoryKey]
      ,[IpAddressLocator]
  FROM [AdventureWorksDW].[dbo].[DimGeography]
GO
/****** Object:  View [dbo].[vProduct]    Script Date: 4/22/2020 9:31:15 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vProduct]
as
SELECT [ProductKey]
      ,[ProductAlternateKey]
      ,[ProductSubcategoryKey]
      ,[WeightUnitMeasureCode]
      ,[SizeUnitMeasureCode]
      ,[EnglishProductName]
      ,[StandardCost]
      ,[FinishedGoodsFlag]
      ,[Color]
      ,[SafetyStockLevel]
      ,[ReorderPoint]
      ,[ListPrice]
      ,[Size]
      ,[SizeRange]
      ,[Weight]
      ,[DaysToManufacture]
      ,[ProductLine]
      ,[DealerPrice]
      ,[Class]
      ,[Style]
      ,[ModelName]
      ,[EnglishDescription]
      ,[StartDate]
      ,[EndDate]
      ,[Status]
  FROM [AdventureWorksDW].[dbo].[DimProduct]
GO
/****** Object:  View [dbo].[vProductCategory]    Script Date: 4/22/2020 9:31:15 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vProductCategory]
as
SELECT TOP (1000) [ProductCategoryKey]
      ,[ProductCategoryAlternateKey]
      ,[EnglishProductCategoryName]
  FROM [AdventureWorksDW].[dbo].[DimProductCategory]
GO
/****** Object:  View [dbo].[vProductSubcategory]    Script Date: 4/22/2020 9:31:15 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vProductSubcategory]
as
SELECT [ProductSubcategoryKey]
      ,[ProductSubcategoryAlternateKey]
      ,[EnglishProductSubcategoryName]
      ,[ProductCategoryKey]
  FROM [AdventureWorksDW].[dbo].[DimProductSubcategory]
GO
/****** Object:  View [dbo].[vSalesTerritory]    Script Date: 4/22/2020 9:31:15 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vSalesTerritory]
as
SELECT [SalesTerritoryKey]
      ,[SalesTerritoryAlternateKey]
      ,[SalesTerritoryRegion]
      ,[SalesTerritoryCountry]
      ,[SalesTerritoryGroup]
      
  FROM [AdventureWorksDW].[dbo].[DimSalesTerritory]
GO
