#!/usr/bin/env python
# coding: utf-8

# ## nb-spark-lake-database-code-v1
# 
# 
# 

# In[1]:


#
# Name:         nb-spark-lake-database-code
#

#
# Design Phase:
#     Author:   John Miner
#     Date:     09-15-2022
#     Purpose:  Data can either be processed into final file or exposed as views.
#         
# Notes:
#     Works for CSV in Spark but SQL Pools has issues with some of the file coding.
#     Instead, use parquet and never have issues.
#


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Create database - saleslt\r\n--\r\n\r\nUSE default;\r\nCREATE DATABASE IF NOT EXISTS saleslt;\r\nUSE saleslt;\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.currency\r\n--\r\n\r\nCREATE TABLE dim_currency\r\n(\r\n    CurrencyKey INT,\r\n\tCurrencyAlternateKey STRING,\r\n\tCurrencyName STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimCurrency.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\r\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.customer\r\n--\r\n\r\nCREATE TABLE dim_customer\r\n(\r\n    CustomerKey INT, \r\n    GeographyKey INT, \r\n    CustomerAlternateKey STRING, \r\n    Title STRING, \r\n    FirstName STRING, \r\n    MiddleName STRING, \r\n    LastName STRING, \r\n    NameStyle INT, \r\n    BirthDate DATE, \r\n    MaritalStatus STRING, \r\n    Suffix STRING, \r\n    Gender STRING, \r\n    EmailAddress STRING, \r\n    YearlyIncome DECIMAL, \r\n    TotalChildren INT, \r\n    NumberChildrenAtHome INT, \r\n    EnglishEducation STRING, \r\n    SpanishEducation STRING, \r\n    FrenchEducation STRING, \r\n    EnglishOccupation STRING, \r\n    SpanishOccupation STRING, \r\n    FrenchOccupation STRING, \r\n    HouseOwnerFlag STRING, \r\n    NumberCarsOwned INT, \r\n    AddressLine1 STRING, \r\n    AddressLine2 STRING, \r\n    Phone STRING, \r\n    DateFirstPurchase DATE, \r\n    CommuteDistance STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimCustomer.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.date\r\n--\r\n\r\nCREATE TABLE dim_date\r\n(\r\n    DateKey INT, \r\n    FullDateAlternateKey TIMESTAMP,\r\n    DayNumberOfWeek SHORT,\r\n    EnglishDayNameOfWeek STRING,\r\n    SpanishDayNameOfWeek STRING,\r\n    FrenchDayNameOfWeek STRING,\r\n    DayNumberOfMonth SHORT,\r\n    DayNumberOfYear SHORT,\r\n    WeekNumberOfYear SHORT,\r\n    EnglishMonthName STRING,\r\n    SpanishMonthName STRING,\r\n    FrenchMonthName STRING,\r\n    MonthNumberOfYear SHORT,\r\n    CalendarQuarter SHORT,\r\n    CalendarYear SHORT,\r\n    CalendarSemester SHORT,\r\n    FiscalQuarter SHORT,\r\n    FiscalYear SHORT,\r\n    FiscalSemester SHORT\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimDate.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.geography\r\n--\r\n\r\nCREATE TABLE dim_geography\r\n(\r\n    GeographyKey INT,\r\n    City STRING,\r\n    StateProvinceCode STRING,\r\n    StateProvinceName STRING,\r\n    CountryRegionCode STRING,\r\n    EnglishCountryRegionName STRING,\r\n    SpanishCountryRegionName STRING,\r\n    FrenchCountryRegionName STRING,\r\n    PostalCode STRING,\r\n    SalesTerritoryKey INT,\r\n    IpAddressLocator STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimGeography.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.product\r\n--\r\n\r\nCREATE TABLE dim_product\r\n(\r\n    ProductKey INTEGER,\r\n\tProductAlternateKey STRING,\r\n\tProductSubcategoryKey INTEGER,\r\n\tWeightUnitMeasureCode STRING,\r\n\tSizeUnitMeasureCode STRING,\r\n\tEnglishProductName STRING,\r\n\tSpanishProductName STRING,\r\n\tFrenchProductName STRING,\r\n\tStandardCost decimal(19,4),\r\n\tFinishedGoodsFlag BOOLEAN,\r\n\tColor STRING,\r\n\tSafetyStockLevel SHORT,\r\n\tReorderPoint SHORT,\r\n\tListPrice decimal(19,4),\r\n\tSize STRING,\r\n\tSizeRange STRING,\r\n\tWeight decimal(19,4),\r\n\tDaysToManufacture INTEGER,\r\n\tProductLine STRING,\r\n\tDealerPrice decimal(19,4),\r\n\tClass STRING,\r\n\tStyle STRING,\r\n\tModelName STRING,\r\n\tStartDate TIMESTAMP,\r\n\tEndDate TIMESTAMP,\r\n\tStatus STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimProduct.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.product_category\r\n--\r\n\r\nCREATE TABLE dim_product_category\r\n(\r\n    ProductCategoryKey INT,\r\n    ProductCategoryAlternateKey INT,\r\n    EnglishProductCategoryName STRING,\r\n    SpanishProductCategoryName STRING,\r\n    FrenchProductCategoryName STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimProductCategory.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.product_subcategory\r\n--\r\n\r\nCREATE TABLE dim_product_subcategory\r\n(\r\n    ProductSubcategoryKey INT,\r\n    ProductSubcategoryAlternateKey INT,\r\n    EnglishProductSubcategoryName STRING,\r\n    SpanishProductSubcategoryName STRING,\r\n    FrenchProductSubcategoryName STRING,\r\n    ProductCategoryKey INT\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimProductSubcategory.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.sales_reason\r\n--\r\n\r\nCREATE TABLE dim_sales_reason\r\n(\r\n    SalesReasonKey INT,\r\n    SalesReasonAlternateKey INT,\r\n    SalesReasonName STRING,\r\n    SalesReasonReasonType STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimSalesReason.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[6]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - dim.sales_territory\r\n--\r\n\r\nCREATE TABLE dim_sales_territory\r\n(\r\n    SalesTerritoryKey INT,\r\n    SalesTerritoryAlternateKey INT,\r\n    SalesTerritoryRegion STRING,\r\n    SalesTerritoryCountry STRING,\r\n    SalesTerritoryGroup STRING\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/DimSalesTerritory.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - fact.internet_sales\r\n--\r\n\r\nCREATE TABLE IF NOT EXISTS fact_internet_sales\r\n(\r\n    ProductKey int,\r\n    OrderDateKey int,\r\n    DueDateKey int,\r\n    ShipDateKey int,\r\n    CustomerKey int,\r\n    PromotionKey int,\r\n    CurrencyKey int,\r\n    SalesTerritoryKey int,\r\n    SalesOrderNumber string,\r\n    SalesOrderLineNumber short,\r\n    RevisionNumber short,\r\n    OrderQuantity short,\r\n    UnitPrice decimal,\r\n    ExtendedAmount decimal,\r\n    UnitPriceDiscountPct decimal,\r\n    DiscountAmount decimal,\r\n    ProductStandardCost decimal,\r\n    TotalProductCost decimal,\r\n    SalesAmount decimal,\r\n    TaxAmt decimal,\r\n    Freight decimal,\r\n    CarrierTrackingNumber string,\r\n    CustomerPONumber string,\r\n    OrderDate timestamp ,\r\n    DueDate timestamp ,\r\n    ShipDate timestamp\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/FactInternetSales.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n--  Create Table - fact.internet_sales_reason\r\n--\r\n\r\nCREATE TABLE fact_internet_sales_reason\r\n(\r\n    SalesOrderNumber STRING,\r\n\tSalesOrderLineNumber SHORT,\r\n\tSalesReasonKey INT\r\n)\r\nUSING CSV\r\nLOCATION \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/csv-files/FactInternetSalesReason.csv\'\r\nOPTIONS \r\n(\r\n    header = "false", \r\n    delimiter = "|"\r\n)\n')


# In[1]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Check table counts\r\n--\r\n\r\n-- https://dataedo.com/samples/html/Data_warehouse/doc/AdventureWorksDW_4/modules/Internet_Sales_101/module.html\r\n\r\nUSE saleslt;\r\n\r\nSELECT 'dim.currency' as Label, COUNT(*) as Total FROM dim_currency\r\nUNION\r\nSELECT 'dim.customer' as Label, COUNT(*) as Total FROM dim_customer \r\nUNION\r\nSELECT 'dim.date' as Label, COUNT(*) as Total FROM dim_date \r\nUNION\r\nSELECT 'dim.geography' as Label, COUNT(*) as Total FROM dim_geography\r\nUNION\r\nSELECT 'dim.product' as Label, COUNT(*) as Total FROM dim_product\r\nUNION\r\nSELECT 'dim.product_category' as Label, COUNT(*) as Total FROM dim_product_category\r\nUNION\r\nSELECT 'dim.product_subcategory' as Label, COUNT(*) as Total FROM dim_product_subcategory\r\nUNION\r\nSELECT 'dim.sales_reason' as Label, COUNT(*) as Total FROM dim_sales_reason\r\nUNION\r\nSELECT 'dim.sales_territory' as Label, COUNT(*) as Total FROM dim_sales_territory\r\nUNION\r\nSELECT 'fact.internet_sales' as Label, COUNT(*) as Total FROM fact_internet_sales \r\nUNION\r\nSELECT 'fact.internet_sales_reason' as Label, COUNT(*) as Total FROM fact_internet_sales_reason \r\n\r\nORDER BY Label\r\n")


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Drop database - saleslt\r\n--\r\n\r\nUSE default;\r\n-- DROP DATABASE saleslt CASCADE;\r\n')


# In[ ]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n-- Create a view \r\n-- \r\n\r\nCREATE VIEW rpt_prepared_data\r\nAS\r\nSELECT\r\n   pc.EnglishProductCategoryName\r\n  ,Coalesce(p.ModelName, p.EnglishProductName) AS Model\r\n  ,c.CustomerKey\r\n  ,s.SalesTerritoryGroup AS Region\r\n  ,CASE\r\n    WHEN month(current_timestamp) < month(c.BirthDate) THEN \r\n      year(c.BirthDate) - year(current_timestamp) - 1\r\n    WHEN month(current_timestamp) = month(c.BirthDate) AND day(current_timestamp) < day(c.BirthDate) THEN \r\n\t  year(c.BirthDate) - year(current_timestamp) - 1\r\n    ELSE \r\n\t    year(c.BirthDate) - year(current_timestamp)\r\n  END AS Age\r\n  ,CASE\r\n      WHEN c.YearlyIncome < 40000 THEN 'Low'\r\n      WHEN c.YearlyIncome > 60000 THEN 'High'\r\n      ELSE 'Moderate'\r\n  END AS IncomeGroup\r\n  ,d.CalendarYear\r\n  ,d.FiscalYear\r\n  ,d.MonthNumberOfYear AS Month\r\n  ,f.SalesOrderNumber AS OrderNumber\r\n  ,f.SalesOrderLineNumber AS LineNumber\r\n  ,f.OrderQuantity AS Quantity\r\n  ,f.ExtendedAmount AS Amount   \r\nFROM\r\n  fact_internet_sales as f\r\nINNER JOIN \r\n  dim_date as d\r\nON \r\n  f.OrderDateKey = d.DateKey\r\n\r\nINNER JOIN \r\n  dim_product as p\r\nON \r\n  f.ProductKey = p.ProductKey\r\n  \r\nINNER JOIN \r\n  dim_product_subcategory as psc\r\nON \r\n  p.ProductSubcategoryKey = psc.ProductSubcategoryKey\r\n\r\nINNER JOIN \r\n  dim_product_category as pc\r\nON \r\n  psc.ProductCategoryKey = pc.ProductCategoryKey\r\n  \r\nINNER JOIN \r\n  dim_customer as c\r\nON \r\n  f.CustomerKey = c.CustomerKey\r\n\r\nINNER JOIN \r\n  dim_geography as g\r\nON \r\n  c.GeographyKey = g.GeographyKey\r\n\r\nINNER JOIN \r\n  dim_sales_territory as s\r\nON \r\n  g.SalesTerritoryKey = s.SalesTerritoryKey \n")


# In[2]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Use view in aggregation\r\n-- \r\n\r\nSELECT \r\n  CalendarYear as RptYear,\r\n  Month as RptMonth,\r\n  Region as RptRegion,\r\n  Model as ModelNo,\r\n  SUM(Quantity) as TotalQty,\r\n  SUM(Amount) as TotalAmt\r\nFROM \r\n  rpt_prepared_data \r\nGROUP BY\r\n  CalendarYear,\r\n  Month,\r\n  Region,\r\n  Model\r\nORDER BY\r\n  CalendarYear,\r\n  Month,\r\n  Region\n')

