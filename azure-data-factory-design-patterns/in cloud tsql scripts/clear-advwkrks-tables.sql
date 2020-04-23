/******************************************************
 *
 * Name:         clear-advwkrks-tables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     04-22-2020
 *     Purpose:  Clear the data from the cloud
 *               reporting database.
 * 
 ******************************************************/

/*
   Truncate all tables except for pipeline settings
*/

TRUNCATE TABLE [dbo].[DimCustomer]
GO

TRUNCATE TABLE [dbo].[DimDate]
GO

TRUNCATE TABLE [dbo].[DimGeography]
GO

TRUNCATE TABLE [dbo].[DimProduct]
GO

TRUNCATE TABLE [dbo].[DimProductCategory]
GO

TRUNCATE TABLE [dbo].[DimProductSubcategory]
GO

TRUNCATE TABLE [dbo].[DimSalesTerritory]
GO

TRUNCATE TABLE [dbo].[FactInternetSales]
GO

TRUNCATE TABLE [stage].[FactInternetSales]
GO

TRUNCATE TABLE [ctrl].[AdfPipelineSettings]
GO

TRUNCATE TABLE [rpt].[SalesByYearMonthRegion]
GO