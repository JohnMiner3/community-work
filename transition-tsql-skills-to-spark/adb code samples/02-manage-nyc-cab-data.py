# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         02-manage-nyc-cab-data
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-15-2020
# MAGIC     Purpose:  Demostrate how to create tables and views.
# MAGIC     
# MAGIC Assumptions:
# MAGIC     None
# MAGIC     
# MAGIC Algorithm:
# MAGIC     1 - Review files in nyc directory. 
# MAGIC     2 - Create database named nyc.    
# MAGIC     
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/databricks-datasets/nyctaxi/readme_nyctaxi.txt

# COMMAND ----------

#
#  List existing hive databases
#

df = spark.sql("SHOW DATABASES")
df.show()



# COMMAND ----------

#
#  Create new database
#

df = spark.sql("create database nyc")
# df = spark.sql("drop database nyc")


# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # Has data integrity issue
# MAGIC # ls /dbfs/databricks-datasets/nyctaxi/tables/nyctaxi_yellow
# MAGIC 
# MAGIC # Reference data
# MAGIC # ls /dbfs/databricks-datasets/nyctaxi/taxizone
# MAGIC 
# MAGIC # Trip data - green, yellow, fhv
# MAGIC # ls /dbfs/databricks-datasets/nyctaxi/tripdata/yellow
# MAGIC # ls /dbfs/databricks-datasets/nyctaxi/tripdata/green
# MAGIC # ls /dbfs/databricks-datasets/nyctaxi/tripdata/fhv
# MAGIC 
# MAGIC # Show part of table
# MAGIC cat /dbfs/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv

# COMMAND ----------




# COMMAND ----------

#
# Remove
#

#dbutils.fs.rm("/rissug/bronze/payment_type/taxi_payment_type-20201215.csv")
#dbutils.fs.rm("/rissug/silver/payment_type/taxi_payment_type.csv")

dbutils.fs.ls("/rissug/silver/payment_type/taxi_payment_type.csv")

# COMMAND ----------

#
#  Create our sample lake - reference data
#

# local variable - which file processing?
section = 1

# code section 1
if (section == 1):
  
  # raw payment type data
  dbutils.fs.mkdirs("/rissug/bronze/payment_type")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv", "/rissug/bronze/payment_type/taxi_payment_type-20201215.csv")

  # refined payment type data
  dbutils.fs.mkdirs("/rissug/silver/payment_type")  
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv", "/rissug/silver/payment_type/taxi_payment_type.csv")

# code section 2
if (section == 2):
  
  # raw rate code data
  dbutils.fs.mkdirs("/rissug/bronze/rate_code")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv", "/rissug/bronze/rate_code/taxi_rate_code-20201215.csv")

  # refined rate code data
  dbutils.fs.mkdirs("/rissug/silver/rate_code")  
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv", "/rissug/silver/rate_code/taxi_rate_code.csv")

  
# code section 3
if (section == 3):
  
  # raw rate code data
  dbutils.fs.mkdirs("/rissug/bronze/zone_lookup")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv", "/rissug/bronze/zone_lookup/taxi_zone_lookup-20201215.csv")

  # refined rate code data
  dbutils.fs.mkdirs("/rissug/silver/zone_lookup")  
  dbutils.fs.cp("/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv", "/rissug/silver/zone_lookup/taxi_zone_lookup.csv")
  


# COMMAND ----------

# MAGIC %sh
# MAGIC # ls /dbfs/rissug/bronze/payment_type
# MAGIC # ls /dbfs/rissug/silver/payment_type
# MAGIC 
# MAGIC # ls /dbfs/rissug/bronze/rate_code
# MAGIC # ls /dbfs/rissug/silver/rate_code
# MAGIC 
# MAGIC # ls /dbfs/rissug/bronze/zone_lookup
# MAGIC # ls /dbfs/rissug/silver/zone_lookup

# COMMAND ----------

#
#  Create payment type table
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.payment_type")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.payment_type
    USING CSV
    LOCATION '/rissug/silver/payment_type/taxi_payment_type.csv'
    OPTIONS (header "True")
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.payment_type 

# COMMAND ----------

#
#  Create rate code table
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.rate_code")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.rate_code
    USING CSV
    LOCATION '/rissug/silver/rate_code/taxi_rate_code.csv'
    OPTIONS (header "True")
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.rate_code

# COMMAND ----------

#
#  Create zone lookup table
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.zone_lookup")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.zone_lookup
    USING CSV
    LOCATION '/rissug/silver/zone_lookup/taxi_zone_lookup.csv'
    OPTIONS (header "True")
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from nyc.zone_lookup limit 5
# MAGIC select count(*) from nyc.zone_lookup

# COMMAND ----------

#
#  Create yellow cab table - bad data - missing rate code
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.yellow_cab")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.yellow_cab
    USING DELTA
    LOCATION '/databricks-datasets/nyctaxi/tables/nyctaxi_yellow'
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC select format_number(count(*), 2) as Total from nyc.yellow_cab 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.yellow_cab where vendor_id = 'CMT' limit 50

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz", header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

#
#  Create our sample lake - trip data
#

# local variable - which file processing?
section = 6

# code section 4
if (section == 4):
  
  # green taxi data (raw csv gz)
  dbutils.fs.mkdirs("/rissug/bronze/green_taxi")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/tripdata/green/", "/rissug/bronze/green_taxi/", recurse=True)
  

  # read into dataframe & write delta file
  df = spark.read.option("mode", "DROPMALFORMED").option("delimiter", ",").csv("/rissug/bronze/green_taxi/*.csv.gz", header=True)
  df = df.withColumnRenamed("Trip_type ", "Trip_type")
  df.write.format("delta").save("/rissug/silver/green_taxi_delta/")
  
# code section 5
if (section == 5):
  
  # yellow taxi data (raw csv gz)
  dbutils.fs.mkdirs("/rissug/bronze/yellow_taxi")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/tripdata/yellow/", "/rissug/bronze/yellow_taxi/", recurse=True)
  
  # read into dataframe & write delta file
  df = spark.read.option("mode", "DROPMALFORMED").option("delimiter", ",").csv("/rissug/bronze/yellow_taxi/*.csv.gz", header=True)
  df = df.withColumnRenamed("Trip_type ", "Trip_type")
  df.write.format("delta").save("/rissug/silver/yellow_taxi_delta/")
  
# code section 6
if (section == 6):
  
  # for hire vehicles (fvh) data (raw csv gz)
  dbutils.fs.mkdirs("/rissug/bronze/fhv")
  dbutils.fs.cp("/databricks-datasets/nyctaxi/tripdata/fhv/", "/rissug/bronze/fhv/", recurse=True)
  
  # read into dataframe & write delta file
  df = spark.read.option("mode", "DROPMALFORMED").option("delimiter", ",").csv("/rissug/bronze/fhv/*.csv.gz", header=True)
  df.write.format("delta").save("/rissug/silver/fhv/")
  


# COMMAND ----------

# MAGIC %sh
# MAGIC # ls /dbfs/rissug/bronze/green_taxi
# MAGIC # ls /dbfs/rissug/silver/green_taxi_delta
# MAGIC 
# MAGIC # ls /dbfs/rissug/bronze/yellow_taxi
# MAGIC # ls /dbfs/rissug/silver/yellow_taxi_delta

# COMMAND ----------

#
#  Create green cab table 
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.green_cab")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.green_cab
    USING DELTA
    LOCATION '/rissug/silver/green_taxi_delta/'
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.green_cab 

# COMMAND ----------

# MAGIC %sql
# MAGIC select format_number(count(*), 2) as Total from nyc.green_cab 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/rissug/silver/yellow_taxi_delta/

# COMMAND ----------

#
#  Create yellow cab table 
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.yellow_cab")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.yellow_cab
    USING DELTA
    LOCATION '/rissug/silver/yellow_taxi_delta/'
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.yellow_cab 

# COMMAND ----------

# MAGIC %sql
# MAGIC select format_number(count(*), 2) as Total from nyc.yellow_cab 

# COMMAND ----------

#
#  Create for hire vehicles table 
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.for_hire_vehicles")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.for_hire_vehicles
    USING DELTA
    LOCATION '/rissug/silver/fhv/'
"""
df = spark.sql(sql_stmt)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.for_hire_vehicles 

# COMMAND ----------

# MAGIC %sql
# MAGIC select format_number(count(*), 2) as Total from nyc.for_hire_vehicles 

# COMMAND ----------

#
# Spark functions
#

# https://spark.apache.org/docs/latest/api/sql/index.html
