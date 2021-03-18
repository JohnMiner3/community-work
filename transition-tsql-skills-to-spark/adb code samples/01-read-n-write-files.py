# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         01-read-n-write-files
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-15-2020
# MAGIC     Purpose:  Reading and writing files.
# MAGIC     
# MAGIC Assumptions:
# MAGIC     None
# MAGIC     
# MAGIC Algorithm:
# MAGIC     1 - Show sample dataset directory.
# MAGIC     2 - Show power plant dataset directory.
# MAGIC     3 - Show content of the 1st tsv file.
# MAGIC     4 - Display the readme file for the power plant sample.
# MAGIC     5 - Read all 4 tsv files into a dataframe.
# MAGIC     6 - Make new directory.
# MAGIC     7 - Display recount count.
# MAGIC     8 - Write dataframe to new directory.
# MAGIC     9 - Display output results.
# MAGIC     
# MAGIC     
# MAGIC ```

# COMMAND ----------

#
# 1 - Sample data sets - all
#


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/databricks-datasets
# MAGIC # ls /dbfs/

# COMMAND ----------

#
# 2 - Sample data sets - power plant
#

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/databricks-datasets/power-plant/data

# COMMAND ----------

#
# 3 - Concatenate files & print output
#

# COMMAND ----------

# MAGIC %%bash
# MAGIC cat /dbfs/databricks-datasets/power-plant/data/Sheet1.tsv

# COMMAND ----------

#
# 4 - View power plant readme
#

path = "/dbfs/databricks-datasets/power-plant/README.md"
with open(path) as file:
  lines = ''.join(file.readlines())
print(lines)

# COMMAND ----------

#
# - Dataset information from uci
#

info = """

Attribute Information:

Features consist of hourly average ambient variables
- Temperature (T) in the range 1.81°C and 37.11°C,
- Ambient Pressure (AP) in the range 992.89-1033.30 milibar,
- Relative Humidity (RH) in the range 25.56% to 100.16%
- Exhaust Vacuum (V) in teh range 25.36-81.56 cm Hg
- Net hourly electrical energy output (EP) 420.26-495.76 MW

The averages are taken from various sensors located around the plant that record the ambient variables every second. 
The variables are given without normalization.

"""


# COMMAND ----------

#
# 5 - Read 4 files into one dataframe
# 

src_path = "/databricks-datasets/power-plant/data/*.tsv"
df_plant = (spark.read 
  .format("csv") 
  .option("header", "true") 
  .option("delimiter", "\t")
  .option("quote", "")
  .option("inferSchema", "true")
  .load(src_path)  
  .repartition(2)
  )
display(df_plant)

# COMMAND ----------

#
# 6 - Make new directories
#

# force remove datalake
dbutils.fs.rm("/rissug", recurse=True)

# simulate datalake quality levels
dbutils.fs.mkdirs("/rissug")
dbutils.fs.mkdirs("/rissug/bronze")
dbutils.fs.mkdirs("/rissug/silver")
dbutils.fs.mkdirs("/rissug/gold")


# COMMAND ----------

#
# Remove from bundle
#

dbutils.fs.ls("/rissug/bronze")
# dbutils.fs.rm("/rissug/bronze/power-plant", recurse=True)

# COMMAND ----------



# COMMAND ----------

#
# 7 - Show record count
#

print("Total number of data points in set is {}.".format(df_plant.count()))


# COMMAND ----------

#
# 8 - Write file to new directory
#

# Write out parquet files
dst_path = "/rissug/bronze/power-plant/"
(
  df_plant
    .write
    .format("parquet")
    .mode("overwrite")
    .save(dst_path)
)

# COMMAND ----------

#
# 9 - List directory contents
#

# Sample datalake
# dbutils.fs.ls("/rissug")

# Current dataset
dbutils.fs.ls("/rissug/bronze/power-plant/")

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

