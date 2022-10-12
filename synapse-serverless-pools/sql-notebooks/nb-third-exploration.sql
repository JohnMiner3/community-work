--
-- Name:         nb-third-exploration
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  Third try at serverless SQL pools
--

--
-- 0 - Create logical database
--

SELECT * FROM sys.databases;
GO

--
-- 1 - Create logical database
--

CREATE DATABASE [mssqltips];
GO

-- set the database
USE [mssqltips]; 
GO


--
-- 2 - adventure works data (parquet files)
--

CREATE SCHEMA [saleslt];
GO


--
-- 3 - create file format
--

CREATE EXTERNAL FILE FORMAT [ParquetFile] 
WITH 
( 
	FORMAT_TYPE = PARQUET
)
GO

-- Delimited files
CREATE EXTERNAL FILE FORMAT [DelimitedFile] 
WITH 
( 
	FORMAT_TYPE = DELIMITEDTEXT ,
	FORMAT_OPTIONS 
	(
		FIELD_TERMINATOR = '|',
		USE_TYPE_DEFAULT = FALSE
	)
)
GO



--
-- 4 - create master key / database scoped credential
--

-- Drop if required
DROP MASTER KEY;
GO

-- Create master key 
CREATE MASTER KEY ENCRYPTION BY PASSWORD='AcEADre5eHJvEcWFhUf8';
GO

-- Drop if required
DROP DATABASE SCOPED CREDENTIAL [LakeCredential];  
GO  

-- Create a database scoped credential.
CREATE DATABASE SCOPED CREDENTIAL [LakeCredential]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<add your shared access key here>';

-- Show credentials
SELECT NAME FROM sys.database_scoped_credentials;
GO


--
-- 5 - create data source
--

-- Drop if required
DROP EXTERNAL DATA SOURCE [LakeDataSource]; 
GO 

-- Create if required
CREATE EXTERNAL DATA SOURCE [LakeDataSource] 
WITH 
(
	LOCATION = 'https://sa4adls2030.dfs.core.windows.net/sc4adls2030/',
	CREDENTIAL = [LakeCredential]
)
GO

-- Show data sources
SELECT * FROM sys.external_data_sources;
GO


--
-- 6 - create table - currency
--

-- Drop if required
DROP EXTERNAL TABLE [saleslt].[dim_currency]
GO 

-- Create table 
CREATE EXTERNAL TABLE [saleslt].[dim_currency]
(
	[CurrencyKey] [int],
	[CurrencyAlternateKey] [nvarchar](3),
	[CurrencyName] [nvarchar](50) 
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimCurrency/*.*.parquet',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO

-- Show data
SELECT * FROM [saleslt].[dim_currency];
GO


--
-- 6 - create table - customer
--

CREATE EXTERNAL TABLE [saleslt].[dim_customer]
(
	[CustomerKey] [int],
	[GeographyKey] [int] ,
	[CustomerAlternateKey] [nvarchar](15),
	[Title] [nvarchar](8) ,
	[FirstName] [nvarchar](50) ,
	[MiddleName] [nvarchar](50) ,
	[LastName] [nvarchar](50) ,
	[NameStyle] [int] ,
	[BirthDate] [date] ,
	[MaritalStatus] [nvarchar](1) ,
	[Suffix] [nvarchar](10) ,
	[Gender] [nvarchar](1) ,
	[EmailAddress] [nvarchar](50) ,
	[YearlyIncome] [money] ,
	[TotalChildren] [int] ,
	[NumberChildrenAtHome] [int] ,
	[EnglishEducation] [nvarchar](40) ,
	[SpanishEducation] [nvarchar](40) ,
	[FrenchEducation] [nvarchar](40) ,
	[EnglishOccupation] [nvarchar](100) ,
	[SpanishOccupation] [nvarchar](100) ,
	[FrenchOccupation] [nvarchar](100) ,
	[HouseOwnerFlag] [nvarchar](1) ,
	[NumberCarsOwned] [int] ,
	[AddressLine1] [nvarchar](120) ,
	[AddressLine2] [nvarchar](120) ,
	[Phone] [nvarchar](20) ,
	[DateFirstPurchase] [date] ,
	[CommuteDistance] [nvarchar](15) 
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimCustomer/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 7 - create table - customer
--

CREATE EXTERNAL TABLE [saleslt].[dim_date]
(
    [DateKey] [int],
	[FullDateAlternateKey] [date],
	[DayNumberOfWeek] [tinyint],
	[EnglishDayNameOfWeek] [nvarchar](10),
	[SpanishDayNameOfWeek] [nvarchar](10),
	[FrenchDayNameOfWeek] [nvarchar](10),
	[DayNumberOfMonth] [tinyint],
	[DayNumberOfYear] [smallint],
	[WeekNumberOfYear] [tinyint],
	[EnglishMonthName] [nvarchar](10),
	[SpanishMonthName] [nvarchar](10),
	[FrenchMonthName] [nvarchar](10),
	[MonthNumberOfYear] [tinyint],
	[CalendarQuarter] [tinyint],
	[CalendarYear] [smallint],
	[CalendarSemester] [tinyint],
	[FiscalQuarter] [tinyint],
	[FiscalYear] [smallint],
	[FiscalSemester] [tinyint]
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimDate/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 8 - create table - geography
--

CREATE EXTERNAL TABLE [saleslt].[dim_geography]
(
	[GeographyKey] [int],
	[City] [nvarchar](30),
	[StateProvinceCode] [nvarchar](3),
	[StateProvinceName] [nvarchar](50),
	[CountryRegionCode] [nvarchar](3),
	[EnglishCountryRegionName] [nvarchar](50),
	[SpanishCountryRegionName] [nvarchar](50),
	[FrenchCountryRegionName] [nvarchar](50),
	[PostalCode] [nvarchar](15),
	[SalesTerritoryKey] [int],
	[IpAddressLocator] [nvarchar](15)
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimGeography/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 9 - create table - product
--

CREATE EXTERNAL TABLE [saleslt].[dim_product]
(
	[ProductKey] [int],
	[ProductAlternateKey] [nvarchar](25),
	[ProductSubcategoryKey] [int],
	[WeightUnitMeasureCode] [nchar](3),
	[SizeUnitMeasureCode] [nchar](3),
	[EnglishProductName] [nvarchar](50),
	[SpanishProductName] [nvarchar](50),
	[FrenchProductName] [nvarchar](50),
	[StandardCost] [money],
	[FinishedGoodsFlag] [bit],
	[Color] [nvarchar](15),
	[SafetyStockLevel] [smallint],
	[ReorderPoint] [smallint],
	[ListPrice] [money],
	[Size] [nvarchar](50),
	[SizeRange] [nvarchar](50),
	[Weight] DECIMAL(19, 4),
	[DaysToManufacture] [int],
	[ProductLine] [nchar](2),
	[DealerPrice] [money],
	[Class] [nchar](2),
	[Style] [nchar](2),
	[ModelName] [nvarchar](50),
	[StartDate] [datetime],
	[EndDate] [datetime],
	[Status] [nvarchar](7) NULL
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimProduct/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 10 - create table - product category
--

CREATE EXTERNAL TABLE [saleslt].[dim_product_category]
(
	[ProductCategoryKey] [int],
	[ProductCategoryAlternateKey] [int],
	[EnglishProductCategoryName] [nvarchar](50),
	[SpanishProductCategoryName] [nvarchar](50),
	[FrenchProductCategoryName] [nvarchar](50) 
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimProductCategory/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO

--
-- 11 - create table - product subcategory
--

CREATE EXTERNAL TABLE [saleslt].[dim_product_subcategory]
(
	[ProductSubcategoryKey] [int],
	[ProductSubcategoryAlternateKey] [int],
	[EnglishProductSubcategoryName] [nvarchar](50),
	[SpanishProductSubcategoryName] [nvarchar](50),
	[FrenchProductSubcategoryName] [nvarchar](50),
	[ProductCategoryKey] [int]
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimProductSubcategory/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 12 - create table - sales reason
--

CREATE EXTERNAL TABLE [saleslt].[dim_sales_reason]
(
    [SalesReasonKey] [int],
	[SalesReasonAlternateKey] [int],
	[SalesReasonName] [nvarchar](50),
	[SalesReasonReasonType] [nvarchar](50) 
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimSalesReason/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO



--
-- 13 - create table - sales territory
--

CREATE EXTERNAL TABLE [saleslt].[dim_sales_territory]
(
	[SalesTerritoryKey] [int],
	[SalesTerritoryAlternateKey] [int],
	[SalesTerritoryRegion] [nvarchar](50),
	[SalesTerritoryCountry] [nvarchar](50),
	[SalesTerritoryGroup] [nvarchar](50)
)
WITH 
(
	LOCATION = 'synapse/parquet-files/DimSalesTerritory/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 13 - create table - fact internet sales
--

CREATE EXTERNAL TABLE [saleslt].[fact_internet_sales]
(
	[ProductKey] [int],
	[OrderDateKey] [int],
	[DueDateKey] [int],
	[ShipDateKey] [int],
	[CustomerKey] [int],
	[PromotionKey] [int],
	[CurrencyKey] [int],
	[SalesTerritoryKey] [int],
	[SalesOrderNumber] [nvarchar](20),
	[SalesOrderLineNumber] [tinyint],
	[RevisionNumber] [tinyint],
	[OrderQuantity] [smallint],
	[UnitPrice] [money],
	[ExtendedAmount] [money],
	[UnitPriceDiscountPct] DECIMAL(19, 4),
	[DiscountAmount] DECIMAL(19, 4),
	[ProductStandardCost] [money],
	[TotalProductCost] [money],
	[SalesAmount] [money],
	[TaxAmt] [money],
	[Freight] [money],
	[CarrierTrackingNumber] [nvarchar](25),
	[CustomerPONumber] [nvarchar](25),
	[OrderDate] [datetime],
	[DueDate] [datetime],
	[ShipDate] [datetime]
)
WITH 
(
	LOCATION = 'synapse/parquet-files/FactInternetSales/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- 14 - create table - fact internet sales reason
--

CREATE EXTERNAL TABLE [saleslt].[fact_internet_sales_reason]
(
	[SalesOrderNumber] [nvarchar](20),
	[SalesOrderLineNumber] [tinyint],
	[SalesReasonKey] [int]
)
WITH 
(
	LOCATION = 'synapse/parquet-files/FactInternetSalesReason/**',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [ParquetFile]
)
GO


--
-- Peak at data
--

-- select top 10 * from [saleslt].[dim_currency]
-- select top 10 * from [saleslt].[dim_customer]
-- select top 10 * from [saleslt].[dim_date]
-- select top 10 * from [saleslt].[dim_geography]
-- select top 10 * from [saleslt].[dim_product]
-- select top 10 * from [saleslt].[dim_product_category]
-- select top 10 * from [saleslt].[dim_product_subcategory]
-- select top 10 * from [saleslt].[dim_sales_reason]
-- select top 10 * from [saleslt].[dim_sales_territory]

-- select top 10 * from [saleslt].[fact_internet_sales]
-- select top 10 * from [saleslt].[fact_internet_sales_reason]


--
-- Remove bad table definition
--

-- drop external table saleslt.fact_internet_sales

-- drop external table saleslt.dim_sales_territory



--
--  15 - Check table counts
--

-- https://dataedo.com/samples/html/Data_warehouse/doc/AdventureWorksDW_4/modules/Internet_Sales_101/module.html


SELECT 'dim.currency' as Label, COUNT(*) as Total FROM [saleslt].[dim_currency]
UNION
SELECT 'dim.customer' as Label, COUNT(*) as Total FROM [saleslt].dim_customer 
UNION
SELECT 'dim.date' as Label, COUNT(*) as Total FROM [saleslt].dim_date 
UNION
SELECT 'dim.geography' as Label, COUNT(*) as Total FROM [saleslt].dim_geography
UNION
SELECT 'dim.product' as Label, COUNT(*) as Total FROM [saleslt].dim_product
UNION
SELECT 'dim.product_category' as Label, COUNT(*) as Total FROM [saleslt].dim_product_category
UNION
SELECT 'dim.product_subcategory' as Label, COUNT(*) as Total FROM [saleslt].dim_product_subcategory
UNION
SELECT 'dim.sales_reason' as Label, COUNT(*) as Total FROM [saleslt].dim_sales_reason
UNION
SELECT 'dim.sales_territory' as Label, COUNT(*) as Total FROM [saleslt].dim_sales_territory
UNION
SELECT 'fact.internet_sales' as Label, COUNT(*) as Total FROM [saleslt].fact_internet_sales 
UNION
SELECT 'fact.internet_sales_reason' as Label, COUNT(*) as Total FROM [saleslt].fact_internet_sales_reason 
ORDER BY Label



--
-- 16 - Create a view 
-- 

CREATE VIEW [saleslt].rpt_prepared_data
AS
SELECT
   pc.EnglishProductCategoryName
  ,Coalesce(p.ModelName, p.EnglishProductName) AS Model
  ,c.CustomerKey
  ,s.SalesTerritoryGroup AS Region
  ,CASE
    WHEN month(current_timestamp) < month(c.BirthDate) THEN 
      year(c.BirthDate) - year(current_timestamp) - 1
    WHEN month(current_timestamp) = month(c.BirthDate) AND day(current_timestamp) < day(c.BirthDate) THEN 
	  year(c.BirthDate) - year(current_timestamp) - 1
    ELSE 
	    year(c.BirthDate) - year(current_timestamp)
  END AS Age
  ,CASE
      WHEN c.YearlyIncome < 40000 THEN 'Low'
      WHEN c.YearlyIncome > 60000 THEN 'High'
      ELSE 'Moderate'
  END AS IncomeGroup
  ,d.CalendarYear
  ,d.FiscalYear
  ,d.MonthNumberOfYear AS Month
  ,f.SalesOrderNumber AS OrderNumber
  ,f.SalesOrderLineNumber AS LineNumber
  ,f.OrderQuantity AS Quantity
  ,f.ExtendedAmount AS Amount   
FROM
  [saleslt].fact_internet_sales as f
INNER JOIN 
  [saleslt].dim_date as d
ON 
  f.OrderDateKey = d.DateKey

INNER JOIN 
  [saleslt].dim_product as p
ON 
  f.ProductKey = p.ProductKey
  
INNER JOIN 
  [saleslt].dim_product_subcategory as psc
ON 
  p.ProductSubcategoryKey = psc.ProductSubcategoryKey

INNER JOIN 
  [saleslt].dim_product_category as pc
ON 
  psc.ProductCategoryKey = pc.ProductCategoryKey
  
INNER JOIN 
  [saleslt].dim_customer as c
ON 
  f.CustomerKey = c.CustomerKey

INNER JOIN 
  [saleslt].dim_geography as g
ON 
  c.GeographyKey = g.GeographyKey

INNER JOIN 
  [saleslt].dim_sales_territory as s
ON 
  g.SalesTerritoryKey = s.SalesTerritoryKey 
GO

-- DROP VIEW [dbo].[rpt_prepared_data]
GO

--
-- 17 - Use view in aggregation
-- 

SELECT 
  CalendarYear as RptYear,
  Month as RptMonth,
  Region as RptRegion,
  Model as ModelNo,
  SUM(Quantity) as TotalQty,
  SUM(Amount) as TotalAmt
FROM 
  [saleslt].rpt_prepared_data 
GROUP BY
  CalendarYear,
  Month,
  Region,
  Model
ORDER BY
  CalendarYear,
  Month,
  Region
