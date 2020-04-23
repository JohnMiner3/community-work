/******************************************************
 *
 * Name:         count-advwkrks-tables.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     04-22-2020
 *     Purpose:  Record count from the cloud
 *               reporting database.
 * 
 ******************************************************/

/*
   Grab a record count from all tables
*/

SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimCustomer]' AS MY_TABLE FROM [dbo].[DimCustomer]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimDate]' AS MY_TABLE FROM [dbo].[DimDate]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimGeography]' AS MY_TABLE FROM [dbo].[DimGeography]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimProduct]' AS MY_TABLE FROM [dbo].[DimProduct]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimProductCategory]' AS MY_TABLE FROM [dbo].[DimProductCategory]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimProductSubcategory]' AS MY_TABLE FROM [dbo].[DimProductSubcategory]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[DimSalesTerritory]' AS MY_TABLE FROM [dbo].[DimSalesTerritory]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[dbo].[FactInternetSales]' AS MY_TABLE FROM [dbo].[FactInternetSales]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[stage].[FactInternetSales]' AS MY_TABLE FROM [stage].[FactInternetSales]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[ctrl].[AdfPipelineSettings]' AS MY_TABLE FROM [ctrl].[AdfPipelineSettings]
UNION ALL
SELECT COUNT(*) AS MY_TOTAL, '[rpt].[SalesByYearMonthRegion]' AS MY_TABLE FROM [rpt].[SalesByYearMonthRegion]
ORDER BY MY_TOTAL
GO
