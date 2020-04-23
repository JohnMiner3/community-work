/****** Object:  Database [dbs4advwrks]    Script Date: 4/22/2020 5:37:21 PM ******/
CREATE DATABASE [dbs4advwrks]  (EDITION = 'Standard', SERVICE_OBJECTIVE = 'S2', MAXSIZE = 250 GB) WITH CATALOG_COLLATION = SQL_Latin1_General_CP1_CI_AS;
GO
ALTER DATABASE [dbs4advwrks] SET COMPATIBILITY_LEVEL = 150
GO
ALTER DATABASE [dbs4advwrks] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [dbs4advwrks] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [dbs4advwrks] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [dbs4advwrks] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [dbs4advwrks] SET ARITHABORT OFF 
GO
ALTER DATABASE [dbs4advwrks] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [dbs4advwrks] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [dbs4advwrks] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [dbs4advwrks] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [dbs4advwrks] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [dbs4advwrks] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [dbs4advwrks] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [dbs4advwrks] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [dbs4advwrks] SET ALLOW_SNAPSHOT_ISOLATION ON 
GO
ALTER DATABASE [dbs4advwrks] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [dbs4advwrks] SET READ_COMMITTED_SNAPSHOT ON 
GO
ALTER DATABASE [dbs4advwrks] SET  MULTI_USER 
GO
ALTER DATABASE [dbs4advwrks] SET ENCRYPTION ON
GO
ALTER DATABASE [dbs4advwrks] SET QUERY_STORE = ON
GO
ALTER DATABASE [dbs4advwrks] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 100, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
/****** Object:  Schema [ctrl]    Script Date: 4/22/2020 5:37:21 PM ******/
CREATE SCHEMA [ctrl]
GO
/****** Object:  Schema [rpt]    Script Date: 4/22/2020 5:37:21 PM ******/
CREATE SCHEMA [rpt]
GO
/****** Object:  Schema [stage]    Script Date: 4/22/2020 5:37:21 PM ******/
CREATE SCHEMA [stage]
GO
/****** Object:  Schema [tmp]    Script Date: 4/22/2020 5:37:21 PM ******/
CREATE SCHEMA [tmp]
GO
/****** Object:  UserDefinedFunction [dbo].[udfBuildISO8601Date]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- See SQL Server Books Online for more details.
CREATE FUNCTION [dbo].[udfBuildISO8601Date] (@year int, @month int, @day int)
RETURNS datetime
AS 
BEGIN
	RETURN cast(convert(varchar, @year) + '-' + [dbo].[udfTwoDigitZeroFill](@month) 
	    + '-' + [dbo].[udfTwoDigitZeroFill](@day) + 'T00:00:00' 
	    as datetime);
END;
GO
/****** Object:  UserDefinedFunction [dbo].[udfTwoDigitZeroFill]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[udfTwoDigitZeroFill] (@number int) 
RETURNS char(2)
AS
BEGIN
	DECLARE @result char(2);
	IF @number > 9 
		SET @result = convert(char(2), @number);
	ELSE
		SET @result = convert(char(2), '0' + convert(varchar, @number));
	RETURN @result;
END;
GO
/****** Object:  Table [dbo].[DimCustomer]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimCustomer](
	[CustomerKey] [int] NULL,
	[GeographyKey] [int] NULL,
	[CustomerAlternateKey] [nvarchar](15) NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [nvarchar](50) NULL,
	[MiddleName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NULL,
	[NameStyle] [bit] NULL,
	[BirthDate] [date] NULL,
	[MaritalStatus] [nchar](1) NULL,
	[Suffix] [nvarchar](10) NULL,
	[Gender] [nvarchar](1) NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[YearlyIncome] [money] NULL,
	[TotalChildren] [tinyint] NULL,
	[NumberChildrenAtHome] [tinyint] NULL,
	[EnglishEducation] [nvarchar](40) NULL,
	[SpanishEducation] [nvarchar](40) NULL,
	[FrenchEducation] [nvarchar](40) NULL,
	[EnglishOccupation] [nvarchar](100) NULL,
	[SpanishOccupation] [nvarchar](100) NULL,
	[FrenchOccupation] [nvarchar](100) NULL,
	[HouseOwnerFlag] [nchar](1) NULL,
	[NumberCarsOwned] [tinyint] NULL,
	[AddressLine1] [nvarchar](120) NULL,
	[AddressLine2] [nvarchar](120) NULL,
	[Phone] [nvarchar](20) NULL,
	[DateFirstPurchase] [date] NULL,
	[CommuteDistance] [nvarchar](15) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimDate]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimDate](
	[DateKey] [int] NULL,
	[FullDateAlternateKey] [date] NULL,
	[DayNumberOfWeek] [tinyint] NULL,
	[EnglishDayNameOfWeek] [nvarchar](10) NULL,
	[SpanishDayNameOfWeek] [nvarchar](10) NULL,
	[FrenchDayNameOfWeek] [nvarchar](10) NULL,
	[DayNumberOfMonth] [tinyint] NULL,
	[DayNumberOfYear] [smallint] NULL,
	[WeekNumberOfYear] [tinyint] NULL,
	[EnglishMonthName] [nvarchar](10) NULL,
	[SpanishMonthName] [nvarchar](10) NULL,
	[FrenchMonthName] [nvarchar](10) NULL,
	[MonthNumberOfYear] [tinyint] NULL,
	[CalendarQuarter] [tinyint] NULL,
	[CalendarYear] [smallint] NULL,
	[CalendarSemester] [tinyint] NULL,
	[FiscalQuarter] [tinyint] NULL,
	[FiscalYear] [smallint] NULL,
	[FiscalSemester] [tinyint] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimProductCategory]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimProductCategory](
	[ProductCategoryKey] [int] NULL,
	[ProductCategoryAlternateKey] [int] NULL,
	[EnglishProductCategoryName] [nvarchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimProductSubcategory]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimProductSubcategory](
	[ProductSubcategoryKey] [int] NULL,
	[ProductSubcategoryAlternateKey] [int] NULL,
	[EnglishProductSubcategoryName] [nvarchar](50) NULL,
	[ProductCategoryKey] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimGeography]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimGeography](
	[GeographyKey] [int] NULL,
	[City] [nvarchar](30) NULL,
	[StateProvinceCode] [nvarchar](3) NULL,
	[StateProvinceName] [nvarchar](50) NULL,
	[CountryRegionCode] [nvarchar](3) NULL,
	[EnglishCountryRegionName] [nvarchar](50) NULL,
	[PostalCode] [nvarchar](15) NULL,
	[SalesTerritoryKey] [int] NULL,
	[IpAddressLocator] [nvarchar](15) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimSalesTerritory]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimSalesTerritory](
	[SalesTerritoryKey] [int] NULL,
	[SalesTerritoryAlternateKey] [int] NULL,
	[SalesTerritoryRegion] [nvarchar](50) NULL,
	[SalesTerritoryCountry] [nvarchar](50) NULL,
	[SalesTerritoryGroup] [nvarchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimProduct]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimProduct](
	[ProductKey] [int] NOT NULL,
	[ProductAlternateKey] [nvarchar](25) NULL,
	[ProductSubcategoryKey] [int] NULL,
	[WeightUnitMeasureCode] [nchar](3) NULL,
	[SizeUnitMeasureCode] [nchar](3) NULL,
	[EnglishProductName] [nvarchar](50) NOT NULL,
	[StandardCost] [money] NULL,
	[FinishedGoodsFlag] [bit] NOT NULL,
	[Color] [nvarchar](15) NOT NULL,
	[SafetyStockLevel] [smallint] NULL,
	[ReorderPoint] [smallint] NULL,
	[ListPrice] [money] NULL,
	[Size] [nvarchar](50) NULL,
	[SizeRange] [nvarchar](50) NULL,
	[Weight] [float] NULL,
	[DaysToManufacture] [int] NULL,
	[ProductLine] [nchar](2) NULL,
	[DealerPrice] [money] NULL,
	[Class] [nchar](2) NULL,
	[Style] [nchar](2) NULL,
	[ModelName] [nvarchar](50) NULL,
	[EnglishDescription] [nvarchar](400) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[Status] [nvarchar](7) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[FactInternetSales]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactInternetSales](
	[ProductKey] [int] NULL,
	[OrderDateKey] [int] NULL,
	[DueDateKey] [int] NULL,
	[ShipDateKey] [int] NULL,
	[CustomerKey] [int] NULL,
	[PromotionKey] [int] NULL,
	[CurrencyKey] [int] NULL,
	[SalesTerritoryKey] [int] NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [tinyint] NOT NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderQuantity] [smallint] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [float] NULL,
	[DiscountAmount] [float] NULL,
	[ProductStandardCost] [money] NULL,
	[TotalProductCost] [money] NULL,
	[SalesAmount] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[CustomerPONumber] [nvarchar](25) NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL,
 CONSTRAINT [PK_FactInternetSales1] PRIMARY KEY CLUSTERED 
(
	[SalesOrderNumber] ASC,
	[SalesOrderLineNumber] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[vDMPrep]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vDMPrep]
AS
    SELECT
        pc.[EnglishProductCategoryName],
        Coalesce(p.[ModelName], p.[EnglishProductName]) AS [Model]
        ,c.[CustomerKey]
        ,s.[SalesTerritoryGroup] AS [Region]
        ,CASE
            WHEN Month(GetDate()) < Month(c.[BirthDate])
                THEN DateDiff(yy,c.[BirthDate],GetDate()) - 1
            WHEN Month(GetDate()) = Month(c.[BirthDate])
            AND Day(GetDate()) < Day(c.[BirthDate])
                THEN DateDiff(yy,c.[BirthDate],GetDate()) - 1
            ELSE DateDiff(yy,c.[BirthDate],GetDate())
        END AS [Age]
        ,CASE
            WHEN c.[YearlyIncome] < 40000 THEN 'Low'
            WHEN c.[YearlyIncome] > 60000 THEN 'High'
            ELSE 'Moderate'
        END AS [IncomeGroup]
        ,d.[CalendarYear]
        ,d.[FiscalYear]
        ,d.[MonthNumberOfYear] AS [Month]
        ,f.[SalesOrderNumber] AS [OrderNumber]
        ,f.SalesOrderLineNumber AS LineNumber
        ,f.OrderQuantity AS Quantity
        ,try_cast(f.ExtendedAmount as real) AS Amount  
    FROM
        [dbo].[FactInternetSales] f
    INNER JOIN [dbo].[DimDate] d
        ON f.[OrderDateKey] = d.[DateKey]
    INNER JOIN [dbo].[DimProduct] p
        ON f.[ProductKey] = p.[ProductKey]
    INNER JOIN [dbo].[DimProductSubcategory] psc
        ON p.[ProductSubcategoryKey] = psc.[ProductSubcategoryKey]
    INNER JOIN [dbo].[DimProductCategory] pc
        ON psc.[ProductCategoryKey] = pc.[ProductCategoryKey]
    INNER JOIN [dbo].[DimCustomer] c
        ON f.[CustomerKey] = c.[CustomerKey]
    INNER JOIN [dbo].[DimGeography] g
        ON c.[GeographyKey] = g.[GeographyKey]
    INNER JOIN [dbo].[DimSalesTerritory] s
        ON g.[SalesTerritoryKey] = s.[SalesTerritoryKey] 
;
GO
/****** Object:  View [dbo].[vTargetMail]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vTargetMail] 
AS
    SELECT
        c.[CustomerKey], 
        c.[GeographyKey], 
        c.[CustomerAlternateKey], 
        c.[Title], 
        c.[FirstName], 
        c.[MiddleName], 
        c.[LastName], 
        c.[NameStyle], 
        c.[BirthDate], 
        c.[MaritalStatus], 
        c.[Suffix], 
        c.[Gender], 
        c.[EmailAddress], 
        c.[YearlyIncome], 
        c.[TotalChildren], 
        c.[NumberChildrenAtHome], 
        c.[EnglishEducation], 
        c.[SpanishEducation], 
        c.[FrenchEducation], 
        c.[EnglishOccupation], 
        c.[SpanishOccupation], 
        c.[FrenchOccupation], 
        c.[HouseOwnerFlag], 
        c.[NumberCarsOwned], 
        c.[AddressLine1], 
        c.[AddressLine2], 
        c.[Phone], 
        c.[DateFirstPurchase], 
        c.[CommuteDistance], 
        x.[Region], 
        x.[Age], 
        CASE x.[Bikes] 
            WHEN 0 THEN 0 
            ELSE 1 
        END AS [BikeBuyer]
    FROM
        [dbo].[DimCustomer] c INNER JOIN (
            SELECT
                [CustomerKey]
                ,[Region]
                ,[Age]
                ,Sum(
                    CASE [EnglishProductCategoryName] 
                        WHEN 'Bikes' THEN 1 
                        ELSE 0 
                    END) AS [Bikes]
            FROM
                [dbo].[vDMPrep] 
            GROUP BY
                [CustomerKey]
                ,[Region]
                ,[Age]
            ) AS [x]
        ON c.[CustomerKey] = x.[CustomerKey]
;
GO
/****** Object:  View [dbo].[vAssocSeqOrders]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vAssocSeqOrders]
AS
SELECT DISTINCT OrderNumber, CustomerKey, Region, IncomeGroup
FROM         dbo.vDMPrep
WHERE     (FiscalYear = '2013')
GO
/****** Object:  View [dbo].[vAssocSeqLineItems]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vAssocSeqLineItems]
AS
SELECT     OrderNumber, LineNumber, Model
FROM         dbo.vDMPrep
WHERE     (FiscalYear = '2013')
GO
/****** Object:  View [dbo].[vTimeSeries]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vTimeSeries] 
AS
    SELECT 
        CASE [Model] 
            WHEN 'Mountain-100' THEN 'M200' 
            WHEN 'Road-150' THEN 'R250' 
            WHEN 'Road-650' THEN 'R750' 
            WHEN 'Touring-1000' THEN 'T1000' 
            ELSE Left([Model], 1) + Right([Model], 3) 
        END + ' ' + [Region] AS [ModelRegion] 
        ,(Convert(Integer, [CalendarYear]) * 100) + Convert(Integer, [Month]) AS [TimeIndex] 
        ,Sum([Quantity]) AS [Quantity] 
        ,Sum([Amount]) AS [Amount]
		,CalendarYear
		,[Month]
		,[dbo].[udfBuildISO8601Date] ([CalendarYear], [Month], 25)
		as ReportingDate
    FROM 
        [dbo].[vDMPrep] 
    WHERE 
        [Model] IN ('Mountain-100', 'Mountain-200', 'Road-150', 'Road-250', 
            'Road-650', 'Road-750', 'Touring-1000') 
    GROUP BY 
        CASE [Model] 
            WHEN 'Mountain-100' THEN 'M200' 
            WHEN 'Road-150' THEN 'R250' 
            WHEN 'Road-650' THEN 'R750' 
            WHEN 'Touring-1000' THEN 'T1000' 
            ELSE Left(Model,1) + Right(Model,3) 
        END + ' ' + [Region] 
        ,(Convert(Integer, [CalendarYear]) * 100) + Convert(Integer, [Month])
		,CalendarYear
		,[Month]
		,[dbo].[udfBuildISO8601Date] ([CalendarYear], [Month], 25);

GO
/****** Object:  View [dbo].[vFactInternetSales]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vFactInternetSales]
as
SELECT [ProductKey]
      ,[OrderDateKey]
      ,[DueDateKey]
      ,[ShipDateKey]
      ,[CustomerKey]
      ,[PromotionKey]
      ,[CurrencyKey]
      ,[SalesTerritoryKey]
      ,[SalesOrderNumber]
      ,[SalesOrderLineNumber]
      ,[RevisionNumber]
      ,[OrderQuantity]
      ,try_cast([UnitPrice] as real) as [UnitPrice]
      ,try_cast([ExtendedAmount] as real) as [ExtendedAmount]
      ,[UnitPriceDiscountPct]
      ,[DiscountAmount]
      ,try_cast([ProductStandardCost] as real) as [ProductStandardCost]
      ,try_cast([TotalProductCost] as real) as [TotalProductCost]
      ,try_cast([SalesAmount] as real) as [SalesAmount]
      ,try_cast([TaxAmt] as real) as [TaxAmt]
      ,try_cast([Freight] as real) as [Freight]
      ,[CarrierTrackingNumber]
      ,[CustomerPONumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
  FROM 
      [dbo].[FactInternetSales]
GO
/****** Object:  Table [stage].[FactInternetSales]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stage].[FactInternetSales](
	[ProductKey] [int] NULL,
	[OrderDateKey] [int] NULL,
	[DueDateKey] [int] NULL,
	[ShipDateKey] [int] NULL,
	[CustomerKey] [int] NULL,
	[PromotionKey] [int] NULL,
	[CurrencyKey] [int] NULL,
	[SalesTerritoryKey] [int] NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [tinyint] NOT NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderQuantity] [smallint] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [float] NULL,
	[DiscountAmount] [float] NULL,
	[ProductStandardCost] [money] NULL,
	[TotalProductCost] [money] NULL,
	[SalesAmount] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[CustomerPONumber] [nvarchar](25) NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL,
 CONSTRAINT [PK_FactInternetSales2] PRIMARY KEY CLUSTERED 
(
	[SalesOrderNumber] ASC,
	[SalesOrderLineNumber] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [stage].[vFactInternetSales]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [stage].[vFactInternetSales]
as
SELECT [ProductKey]
      ,[OrderDateKey]
      ,[DueDateKey]
      ,[ShipDateKey]
      ,[CustomerKey]
      ,[PromotionKey]
      ,[CurrencyKey]
      ,[SalesTerritoryKey]
      ,[SalesOrderNumber]
      ,[SalesOrderLineNumber]
      ,[RevisionNumber]
      ,[OrderQuantity]
      ,try_cast([UnitPrice] as real) as [UnitPrice]
      ,try_cast([ExtendedAmount] as real) as [ExtendedAmount]
      ,[UnitPriceDiscountPct]
      ,[DiscountAmount]
      ,try_cast([ProductStandardCost] as real) as [ProductStandardCost]
      ,try_cast([TotalProductCost] as real) as [TotalProductCost]
      ,try_cast([SalesAmount] as real) as [SalesAmount]
      ,try_cast([TaxAmt] as real) as [TaxAmt]
      ,try_cast([Freight] as real) as [Freight]
      ,[CarrierTrackingNumber]
      ,[CustomerPONumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
  FROM 
      [stage].[FactInternetSales]
GO
/****** Object:  Table [ctrl].[AdfPipelineSettings]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ctrl].[AdfPipelineSettings](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[SRC_SCHEMA_NM] [varchar](10) NULL,
	[SRC_TABLE_NM] [varchar](50) NULL,
	[LAKE_DIR_NM] [varchar](100) NULL,
	[LAKE_PATH_NM] [varchar](100) NULL,
	[LAKE_FILE_NM] [varchar](100) NULL,
	[TRG_SCHEMA_NM] [varchar](10) NULL,
	[TRG_TABLE_NM] [varchar](50) NULL,
	[LOAD_TYPE] [varchar](15) NULL,
	[ACTIVE_FLG] [varchar](1) NULL,
	[POST_TSQL_TXT] [varchar](100) NULL,
	[WATER_MARK_TXT] [varchar](50) NULL,
 CONSTRAINT [PK_ADF_PIPELINE_SETTINGS] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [rpt].[SalesByYearMonthRegion]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [rpt].[SalesByYearMonthRegion](
	[RptYear] [smallint] NULL,
	[RptMonth] [tinyint] NULL,
	[RptRegion] [nvarchar](50) NULL,
	[ModelNo] [nvarchar](50) NULL,
	[TotalQty] [int] NULL,
	[TotalAmt] [money] NULL
) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [stage].[UpsertFactInternetSales]    Script Date: 4/22/2020 5:37:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Create stage table
CREATE   PROCEDURE [stage].[UpsertFactInternetSales]
AS
BEGIN
	-- Set no count
	SET NOCOUNT ON 

	-- Merge the clean stage data with active table
	MERGE 
		[dbo].[FactInternetSales] AS trg
	USING 
	(
		SELECT * FROM [stage].[FactInternetSales]
	) AS src 
	ON 
		src.[SalesOrderNumber] = trg.[SalesOrderNumber] and
		src.[SalesOrderLineNumber] = trg.[SalesOrderLineNumber]
		
     -- Update condition
     WHEN MATCHED THEN 
         UPDATE SET
			[ProductKey] = src.[ProductKey],
			[OrderDateKey] = src.[OrderDateKey],
			[DueDateKey] = src.[DueDateKey],
			[ShipDateKey] = src.[ShipDateKey],
			[CustomerKey] = src.[CustomerKey],
			[PromotionKey] = src.[PromotionKey],
			[CurrencyKey] = src.[CurrencyKey],
			[SalesTerritoryKey] = src.[SalesTerritoryKey],
			[RevisionNumber] = src.[RevisionNumber],
			[OrderQuantity] = src.[OrderQuantity],
			[UnitPrice] = src.[UnitPrice],
			[ExtendedAmount] = src.[ExtendedAmount],
			[UnitPriceDiscountPct] = src.[UnitPriceDiscountPct],
			[DiscountAmount] = src.[DiscountAmount],
			[ProductStandardCost] = src.[ProductStandardCost],
			[TotalProductCost] = src.[TotalProductCost],
			[SalesAmount] = src.[SalesAmount],
			[TaxAmt] = src.[TaxAmt],
			[Freight] = src.[Freight],
			[CarrierTrackingNumber] = src.[CarrierTrackingNumber],
			[CustomerPONumber] = src.[CustomerPONumber],
			[OrderDate] = src.[OrderDate],
			[DueDate] = src.[DueDate],
			[ShipDate] = src.[ShipDate]
 
     -- Insert condition
     WHEN NOT MATCHED BY TARGET THEN
		 INSERT 
         (
		    [ProductKey]
           ,[OrderDateKey]
           ,[DueDateKey]
           ,[ShipDateKey]
           ,[CustomerKey]
           ,[PromotionKey]
           ,[CurrencyKey]
           ,[SalesTerritoryKey]
           ,[SalesOrderNumber]
           ,[SalesOrderLineNumber]
           ,[RevisionNumber]
           ,[OrderQuantity]
           ,[UnitPrice]
           ,[ExtendedAmount]
           ,[UnitPriceDiscountPct]
           ,[DiscountAmount]
           ,[ProductStandardCost]
           ,[TotalProductCost]
           ,[SalesAmount]
           ,[TaxAmt]
           ,[Freight]
           ,[CarrierTrackingNumber]
           ,[CustomerPONumber]
           ,[OrderDate]
           ,[DueDate]
           ,[ShipDate]
		 )
         VALUES
         ( 
			src.[ProductKey],
			src.[OrderDateKey],
			src.[DueDateKey],
			src.[ShipDateKey],
			src.[CustomerKey],
			src.[PromotionKey],
			src.[CurrencyKey],
			src.[SalesTerritoryKey],
			src.[SalesOrderNumber],
			src.[SalesOrderLineNumber],
			src.[RevisionNumber],
			src.[OrderQuantity],
			src.[UnitPrice],
			src.[ExtendedAmount],
			src.[UnitPriceDiscountPct],
			src.[DiscountAmount],
			src.[ProductStandardCost],
			src.[TotalProductCost],
			src.[SalesAmount],
			src.[TaxAmt],
			src.[Freight],
			src.[CarrierTrackingNumber],
			src.[CustomerPONumber],
			src.[OrderDate],
			src.[DueDate],
			src.[ShipDate]
         );

	-- Local variable
	DECLARE @VAR_WATER_MARK_TXT VARCHAR(50);

	-- Max order date key
	SELECT 
		@VAR_WATER_MARK_TXT = CAST(MAX(OrderDateKey) as varchar(50))
	FROM 
		[dbo].[FactInternetSales];

	-- Update the watermark
	UPDATE C
	SET 
		WATER_MARK_TXT = @VAR_WATER_MARK_TXT
	FROM 
		[ctrl].[AdfPipelineSettings] C
	WHERE 
		C.SRC_SCHEMA_NM = 'dbo' AND
		C.SRC_TABLE_NM = 'FactInternetSales';

END
GO
ALTER DATABASE [dbs4advwrks] SET  READ_WRITE 
GO
