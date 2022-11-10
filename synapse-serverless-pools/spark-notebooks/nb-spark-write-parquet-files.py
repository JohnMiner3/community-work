#!/usr/bin/env python
# coding: utf-8

# ## nb-spark-write-parquet-files
# 
# 
# 

# In[1]:


#
# Name:         nb-spark-write-parquet-files
#

#
# Design Phase:
#     Author:   John Miner
#     Date:     09-15-2022
#     Purpose:  Read for lake database and write to parquet files
#


# In[2]:


get_ipython().run_cell_magic('pyspark', '', '\n#\n#  Save parquet file - /currency\n#\n\npath = \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimCurrency\'\nsql = "SELECT * FROM saleslt.dim_currency"\ndf = spark.sql(sql)\ndf.repartition(1).write.parquet(path)\n')


# In[3]:


get_ipython().run_cell_magic('pyspark', '', '\r\n#\r\n#  Save parquet file - /customer\r\n#\r\n\r\npath = \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimCustomer\'\r\nsql = "SELECT * FROM saleslt.dim_customer"\r\ndf = spark.sql(sql)\r\ndf.repartition(1).write.parquet(path)\n')


# In[4]:


get_ipython().run_cell_magic('pyspark', '', '\r\n#\r\n#  Save parquet file - /date\r\n#\r\n\r\npath = \'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimDate\'\r\nsql = "SELECT * FROM saleslt.dim_date"\r\ndf = spark.sql(sql)\r\ndf.repartition(1).write.parquet(path)\n')


# In[5]:


#
#  Save parquet file - /geography
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimGeography'
sql = "SELECT * FROM saleslt.dim_geography"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[6]:


#
#  Save parquet file - /product
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProduct'
sql = "SELECT * FROM saleslt.dim_product"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[7]:


#
#  Save parquet file - /product-category
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProductCategory'
sql = "SELECT * FROM saleslt.dim_product_category"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[8]:


#
#  Save parquet file - /product-subcategory
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimProductSubcategory'
sql = "SELECT * FROM saleslt.dim_product_subcategory"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[9]:


#
#  Save parquet file - /sales-reason
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimSalesReason'
sql = "SELECT * FROM saleslt.dim_sales_reason"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[2]:


#
#  Save parquet file - /sales-territory
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/DimSalesTerritory'
sql = "SELECT * FROM saleslt.dim_sales_territory"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[11]:


#
#  Save parquet file - /fact-internet-sales
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/FactInternetSales'
sql = "SELECT * FROM saleslt.fact_internet_sales"
df = spark.sql(sql)
df.repartition(4).write.parquet(path)


# In[12]:


#
#  Save parquet file - /fact-internet-sales-reason
#

path = 'abfss://sc4adls2030@sa4adls2030.dfs.core.windows.net/synapse/parquet-files/FactInternetSalesReason'
sql = "SELECT * FROM saleslt.fact_internet_sales_reason"
df = spark.sql(sql)
df.repartition(1).write.parquet(path)


# In[ ]:




