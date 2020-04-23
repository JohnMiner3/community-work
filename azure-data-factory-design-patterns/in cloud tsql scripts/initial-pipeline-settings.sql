/******************************************************
 *
 * Name:         initial-pipeline-settings.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     04-22-2020
 *     Purpose:  The Adf Pipeline Settings table controls
 *               the table driven process.
 * 
 ******************************************************/

-- Clear the table
TRUNCATE TABLE [ctrl].[AdfPipelineSettings]
GO

-- Customer table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'DimCustomer',
    'sc4adls2020',
    'datalake/bronze/dim/customer',
    'dbo.dimcustomer',
	'dbo',
    'DimCustomer',
    'F',
    'Y'
)
GO

-- Date table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'DimDate',
    'sc4adls2020',
    'datalake/bronze/dim/date',
    'dbo.dimdate',
	'dbo',
    'DimDate',
    'F',
    'Y'
)
GO

-- Geography table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'vGeography',
    'sc4adls2020',
    'datalake/bronze/dim/geography',
    'dbo.dimgeography',
	'dbo',
    'DimGeography',
    'F',
    'Y'
)
GO

-- Product table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'vProduct',
    'sc4adls2020',
    'datalake/bronze/dim/product',
    'dbo.dimproduct',
	'dbo',
    'DimProduct',
    'F',
    'Y'
)
GO

-- Product Category table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'vProductCategory',
    'sc4adls2020',
    'datalake/bronze/dim/productcategory',
    'dbo.dimproductcategory',
	'dbo',
    'DimProductCategory',
    'F',
    'Y'
)
GO

-- Product Subcategory table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'vProductSubcategory',
    'sc4adls2020',
    'datalake/bronze/dim/productsubcategory',
    'dbo.dimproductsubcategory',
	'dbo',
    'DimProductSubcategory',
    'F',
    'Y'
)
GO

-- Sales territory table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
)
VALUES
(
	'dbo',
    'vSalesTerritory',
    'sc4adls2020',
    'datalake/bronze/dim/salesterritory',
    'dbo.dimsalesterritory',
	'dbo',
    'DimSalesTerritory',
    'F',
    'Y'
)
GO

-- Internet Sales table entry
INSERT INTO [ctrl].[AdfPipelineSettings]
(
	[SRC_SCHEMA_NM]
    ,[SRC_TABLE_NM]
    ,[LAKE_DIR_NM]
    ,[LAKE_PATH_NM]
    ,[LAKE_FILE_NM]
    ,[TRG_SCHEMA_NM]
    ,[TRG_TABLE_NM]
    ,[LOAD_TYPE]
    ,[ACTIVE_FLG]
	,[POST_TSQL_TXT]
    ,[WATER_MARK_TXT]
)
VALUES
(
	'dbo',
    'FactInternetSales',
    'sc4adls2020',
    'datalake/bronze/fact/internetsales',
    'dbo.factinternetsales',
	'dbo',
    'FactInternetSales',
    'F',
    'Y',
	'[stage].[UpsertFactInternetSales]',
	'19000101'
)
GO