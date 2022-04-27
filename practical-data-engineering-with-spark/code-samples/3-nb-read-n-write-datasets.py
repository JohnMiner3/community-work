# Databricks notebook source
# MAGIC %md
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         3-nb-read-n-write-data
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-01-2020
# MAGIC     Purpose:  How to read and write with spark dataframes
# MAGIC 
# MAGIC Learning Guide:
# MAGIC     1 - Create local data lake
# MAGIC     2 - Read + Write CSV files (infer data)
# MAGIC     3 - Read + Write CSV files (infer data)
# MAGIC     4 - Read + Write Parquet Files
# MAGIC     5 - Read + Write TSV files
# MAGIC     
# MAGIC ```

# COMMAND ----------

# MAGIC %run "./n-tool-box-code"

# COMMAND ----------

#
# 1 - Create sample data lake
#

# remove existing
try:
  dbutils.fs.rm("/lake", recurse=True)  
except:
  pass

# root
dbutils.fs.mkdirs("/lake")

# amazon data
dbutils.fs.mkdirs("/lake/bronze/amazon")

# diamond data
dbutils.fs.mkdirs("/lake/bronze/diamonds")

# power plant
dbutils.fs.mkdirs("/lake/bronze/power")

# weather
dbutils.fs.mkdirs("/lake/bronze/weather")

# folders
dbutils.fs.ls("/lake/bronze")



# COMMAND ----------

#
# 2 - Read + write csv data - no schema
#


# COMMAND ----------

# Two files
dbutils.fs.ls("/databricks-datasets/weather")


# COMMAND ----------

# low temps
dbutils.fs.head("/databricks-datasets/weather/low_temps", 500)


# COMMAND ----------

# high temps
dbutils.fs.head("/databricks-datasets/weather/high_temps", 500)


# COMMAND ----------

# read in low temps
path1 = "/databricks-datasets/weather/low_temps"
df1 = (
  spark.read                    
  .option("sep", ",")        
  .option("header", "true")
  .option("inferSchema", "true")  
  .csv(path1)               
)

# COMMAND ----------

# rename columns
df1 = df1.withColumnRenamed("temp", "low_temp")


# COMMAND ----------

# show top 5 rows
display(df1.head(5))

# COMMAND ----------

# read in high temps
path2 = "/databricks-datasets/weather/high_temps"
df2 = (
  spark.read                    
  .option("sep", ",")        
  .option("header", "true")
  .option("inferSchema", "true")  
  .csv(path2)               
)

# COMMAND ----------

# rename columns
df2 = df2.withColumnRenamed("temp", "high_temp")
df2 = df2.withColumnRenamed("date", "date2")


# COMMAND ----------

# show top 5 rows
display(df2.head(5))

# COMMAND ----------

# join + drop col
df3 = df1.join(df2, df1["date"] == df2["date2"]).drop("date2")

# COMMAND ----------

# show top 5 rows
display(df3.head(5))


# COMMAND ----------

# Number of partitions
df3.rdd.getNumPartitions()

# COMMAND ----------

# Write out csv file
dst_path = "/lake/bronze/weather/temp"
(
  df3.repartition(1).write
    .format("csv")
    .mode("overwrite")
    .save(dst_path)
)

# COMMAND ----------

# show results
dbutils.fs.ls("/lake/bronze/weather/temp")

# COMMAND ----------



# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/weather/temp/", "/lake/bronze/weather/temperature-data.csv", "csv")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/weather/")

# COMMAND ----------



# COMMAND ----------

#
# 3 - Read + write csv data - schema defined
#


# COMMAND ----------

# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# https://vincent.doba.fr/posts/20211004_spark_data_description_language_for_defining_spark_schema/

# define DDL
schema = "_c0 INTEGER, carat DOUBLE, cut STRING, color STRING, clarity STRING, depth DOUBLE, table DOUBLE, price INTEGER, x DOUBLE, y DOUBLE, z DOUBLE" 

# specify path
path = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

# read in file
df = (spark.read.format("csv").option("header", "true").schema(schema).load(path))


# COMMAND ----------

# show top 5 rows
display(df.head(5))

# COMMAND ----------

# Write out csv file
dst_path = "/lake/bronze/diamonds/temp"
(
  df.repartition(1).write
    .format("parquet")
    .mode("overwrite")
    .save(dst_path)
)

# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/diamonds/temp/", "/lake/bronze/diamonds/diamonds-data.parquet", "parquet")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/diamonds/")

# COMMAND ----------



# COMMAND ----------

#
# 4 - Read + write parquet file format
#


# COMMAND ----------

# Show files
lst = dbutils.fs.ls("/databricks-datasets/amazon/test4K")
print(lst, "\n\n", len(lst))


# COMMAND ----------

# read in amazon product data
path4 = "/databricks-datasets/amazon/test4K"
df4 = (
  spark.read                    
  .parquet(path4)               
)

# COMMAND ----------

display(df4.head(500))

# COMMAND ----------

# reduce columns
df5 = df4.select("asin", "brand", "price", "rating")

# COMMAND ----------

# Write out csv file
dst_path = "/lake/bronze/amazon/temp"
(
  df5.repartition(1).write
    .format("parquet")
    .mode("overwrite")
    .save(dst_path)
)

# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/amazon/temp/", "/lake/bronze/amazon/product-data.parquet", "parquet")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/amazon/")

# COMMAND ----------



# COMMAND ----------

#
# 5 - Read + write tsv file format
#


# COMMAND ----------


# Data types

# Define schema
src_schema = "AT FLOAT, V FLOAT, AP FLOAT, RH FLOAT, PE FLOAT"
src_path = "/databricks-datasets/power-plant/data/*.tsv"

df6 = (spark.read 
  .format("csv") 
  .schema(src_schema)
  .option("header", "true") 
  .option("delimiter", "\t")
  .option("quote", "")
  .load(src_path)  
  .repartition(1)
  )
display(df6)

# COMMAND ----------

# Write out csv file
path = "/lake/bronze/power/temp"
(
  df6.repartition(1).write
    .format("parquet")
    .mode("overwrite")
    .save(path)
)

# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/power/temp/", "/lake/bronze/power/plant-data.parquet", "parquet")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/power/")

# COMMAND ----------



# COMMAND ----------

#
#  Support file formats
#

# https://spark.apache.org/docs/latest/sql-data-sources.html

# AVRO
# CSV
# JSON
# PARQUET
# TEXT
# JDBC

