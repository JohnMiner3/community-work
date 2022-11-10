#!/usr/bin/env python
# coding: utf-8

# ## nb-spark-lake-database-code-v2
# 
# Use parquet files.
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


# In[4]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Drop database - saleslt\r\n--\r\n\r\nUSE default;\r\nDROP DATABASE IF EXISTS saleslt CASCADE;\n')


# In[5]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Create database - saleslt\r\n--\r\n\r\nCREATE DATABASE IF NOT EXISTS saleslt;\r\nUSE saleslt;\n')


# In[6]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.currency\r\n--\r\n\r\nCREATE TABLE dim_currency\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimCurrency';\n")


# In[7]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.customer\r\n--\r\n\r\nCREATE TABLE dim_customer\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimCustomer';\r\n")


# In[9]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.date\r\n--\r\n\r\nCREATE TABLE dim_date\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimDate';\r\n")


# In[10]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.geography\r\n--\r\n\r\nCREATE TABLE dim_geography\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimGeography';\r\n\r\n")


# In[11]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.product\r\n--\r\n\r\n\r\nCREATE TABLE dim_product\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProduct';\r\n\r\n")


# In[12]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.product_category\r\n--\r\n\r\n\r\nCREATE TABLE dim_product_category\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProductCategory';\r\n")


# In[13]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.product_subcategory\r\n--\r\n\r\nCREATE TABLE dim_product_subcategory\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProductSubcategory';\r\n")


# In[14]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.sales_reason\r\n--\r\n\r\nCREATE TABLE dim_sales_reason\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimSalesReason';\r\n")


# In[15]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - dim.sales_territory\r\n--\r\n\r\nCREATE TABLE dim_sales_territory\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimSalesTerritory';\r\n")


# In[16]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - fact.internet_sales\r\n--\r\n\r\nCREATE TABLE fact_internet_sales\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/FactInternetSales';\r\n")


# In[17]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Create Table - fact.internet_sales_reason\r\n--\r\n\r\nCREATE TABLE fact_internet_sales_reason\r\nUSING PARQUET\r\nLOCATION 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/FactInternetSalesReason';\r\n")


# In[18]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n--  Check table counts\r\n--\r\n\r\n-- https://dataedo.com/samples/html/Data_warehouse/doc/AdventureWorksDW_4/modules/Internet_Sales_101/module.html\r\n\r\nUSE saleslt;\r\n\r\nSELECT 'dim.currency' as Label, COUNT(*) as Total FROM dim_currency\r\nUNION\r\nSELECT 'dim.customer' as Label, COUNT(*) as Total FROM dim_customer \r\nUNION\r\nSELECT 'dim.date' as Label, COUNT(*) as Total FROM dim_date \r\nUNION\r\nSELECT 'dim.geography' as Label, COUNT(*) as Total FROM dim_geography\r\nUNION\r\nSELECT 'dim.product' as Label, COUNT(*) as Total FROM dim_product\r\nUNION\r\nSELECT 'dim.product_category' as Label, COUNT(*) as Total FROM dim_product_category\r\nUNION\r\nSELECT 'dim.product_subcategory' as Label, COUNT(*) as Total FROM dim_product_subcategory\r\nUNION\r\nSELECT 'dim.sales_reason' as Label, COUNT(*) as Total FROM dim_sales_reason\r\nUNION\r\nSELECT 'dim.sales_territory' as Label, COUNT(*) as Total FROM dim_sales_territory\r\nUNION\r\nSELECT 'fact.internet_sales' as Label, COUNT(*) as Total FROM fact_internet_sales \r\nUNION\r\nSELECT 'fact.internet_sales_reason' as Label, COUNT(*) as Total FROM fact_internet_sales_reason \r\n\r\nORDER BY Label\r\n")


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Drop database - saleslt\r\n--\r\n\r\nUSE default;\r\n-- DROP DATABASE saleslt CASCADE;\r\n')


# In[19]:


get_ipython().run_cell_magic('sql', '', "\r\n--\r\n-- Create a view \r\n-- \r\n\r\nCREATE VIEW rpt_prepared_data\r\nAS\r\nSELECT\r\n   pc.EnglishProductCategoryName\r\n  ,Coalesce(p.ModelName, p.EnglishProductName) AS Model\r\n  ,c.CustomerKey\r\n  ,s.SalesTerritoryGroup AS Region\r\n  ,CASE\r\n    WHEN month(current_timestamp) < month(c.BirthDate) THEN \r\n      year(c.BirthDate) - year(current_timestamp) - 1\r\n    WHEN month(current_timestamp) = month(c.BirthDate) AND day(current_timestamp) < day(c.BirthDate) THEN \r\n\t  year(c.BirthDate) - year(current_timestamp) - 1\r\n    ELSE \r\n\t    year(c.BirthDate) - year(current_timestamp)\r\n  END AS Age\r\n  ,CASE\r\n      WHEN c.YearlyIncome < 40000 THEN 'Low'\r\n      WHEN c.YearlyIncome > 60000 THEN 'High'\r\n      ELSE 'Moderate'\r\n  END AS IncomeGroup\r\n  ,d.CalendarYear\r\n  ,d.FiscalYear\r\n  ,d.MonthNumberOfYear AS Month\r\n  ,f.SalesOrderNumber AS OrderNumber\r\n  ,f.SalesOrderLineNumber AS LineNumber\r\n  ,f.OrderQuantity AS Quantity\r\n  ,f.ExtendedAmount AS Amount   \r\nFROM\r\n  fact_internet_sales as f\r\nINNER JOIN \r\n  dim_date as d\r\nON \r\n  f.OrderDateKey = d.DateKey\r\n\r\nINNER JOIN \r\n  dim_product as p\r\nON \r\n  f.ProductKey = p.ProductKey\r\n  \r\nINNER JOIN \r\n  dim_product_subcategory as psc\r\nON \r\n  p.ProductSubcategoryKey = psc.ProductSubcategoryKey\r\n\r\nINNER JOIN \r\n  dim_product_category as pc\r\nON \r\n  psc.ProductCategoryKey = pc.ProductCategoryKey\r\n  \r\nINNER JOIN \r\n  dim_customer as c\r\nON \r\n  f.CustomerKey = c.CustomerKey\r\n\r\nINNER JOIN \r\n  dim_geography as g\r\nON \r\n  c.GeographyKey = g.GeographyKey\r\n\r\nINNER JOIN \r\n  dim_sales_territory as s\r\nON \r\n  g.SalesTerritoryKey = s.SalesTerritoryKey \n")


# In[20]:


get_ipython().run_cell_magic('sql', '', '\r\n--\r\n-- Use view in aggregation\r\n-- \r\n\r\nSELECT \r\n  CalendarYear as RptYear,\r\n  Month as RptMonth,\r\n  Region as RptRegion,\r\n  Model as ModelNo,\r\n  SUM(Quantity) as TotalQty,\r\n  SUM(Amount) as TotalAmt\r\nFROM \r\n  rpt_prepared_data \r\nGROUP BY\r\n  CalendarYear,\r\n  Month,\r\n  Region,\r\n  Model\r\nORDER BY\r\n  CalendarYear,\r\n  Month,\r\n  Region\n')

