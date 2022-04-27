# Databricks notebook source
# MAGIC %md
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         4-use-of-temporary-views
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-01-2020
# MAGIC     Purpose:  Exposing files as views
# MAGIC 
# MAGIC Learning Guide:
# MAGIC     1 - Add new directory
# MAGIC     2 - Process weather data using sql
# MAGIC     3 - Process loan data using sql
# MAGIC     
# MAGIC ```

# COMMAND ----------

# MAGIC %run "./n-tool-box-code"

# COMMAND ----------

#
# 1 - add directory
#

# show directories
dbutils.fs.ls("/lake/bronze")

# make new directory
dbutils.fs.mkdirs("/lake/bronze/loan")

# show directories
dbutils.fs.ls("/lake/bronze")



# COMMAND ----------



# COMMAND ----------

#
# 2 - Weather Data
#

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

# create temp view
df1.createOrReplaceTempView("tmp_low_temps")

# COMMAND ----------

# read in low temps
path2 = "/databricks-datasets/weather/high_temps"
df2 = (
  spark.read                    
  .option("sep", ",")        
  .option("header", "true")
  .option("inferSchema", "true")  
  .csv(path2)               
)

# COMMAND ----------

# create temp view
df2.createOrReplaceTempView("tmp_high_temps")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   l.date as obs_date,
# MAGIC   h.temp as obs_high_temp,
# MAGIC   l.temp as obs_low_temp
# MAGIC from 
# MAGIC   tmp_high_temps as h
# MAGIC join
# MAGIC   tmp_low_temps as l
# MAGIC on
# MAGIC   h.date = l.date

# COMMAND ----------

# make sql string
sql_stmt = """
  select 
    l.date as obs_date,
    h.temp as obs_high_temp,
    l.temp as obs_low_temp
  from 
    tmp_high_temps as h
  join
    tmp_low_temps as l
  on
    h.date = l.date
"""

# execute
df = spark.sql(sql_stmt)

# COMMAND ----------

# Write out csv file
path = "/lake/bronze/weather/temp"
(
  df.repartition(1).write
    .format("parquet")
    .mode("overwrite")
    .save(path)
)

# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/weather/temp/", "/lake/bronze/weather/temperature-data.parquet", "parquet")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/weather/")

# COMMAND ----------



# COMMAND ----------

#
# 3 - Loan Data
#

# COMMAND ----------

# read in low temps
path3 = "/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv"
df3 = (
  spark.read                    
  .option("sep", ",")        
  .option("header", "true")
  .option("inferSchema", "true")  
  .csv(path3)               
)

# COMMAND ----------

# create temp view
df3.createOrReplaceTempView("tmp_loan_club")


# COMMAND ----------

df3.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_loan_club

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   loan_status, 
# MAGIC   cast(regexp_replace(int_rate, '%', '') as float) as int_rate,
# MAGIC   cast(regexp_replace(revol_util, '%', '') as float) as revol_util,
# MAGIC   cast(substring(issue_d, 5, 4) as double) as issue_year,
# MAGIC   cast(substring(earliest_cr_line, 5, 4) as double) as earliest_year,
# MAGIC   cast(substring(issue_d, 5, 4) as double) -
# MAGIC   cast(substring(earliest_cr_line, 5, 4) as double) as credit_length_in_years,
# MAGIC   cast(regexp_replace(regexp_replace(regexp_replace(emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", ""), "< 1", "0"), "10\\+", "10") as float) as emp_length,
# MAGIC   verification_status, 
# MAGIC   total_pymnt,
# MAGIC   loan_amnt, 
# MAGIC   grade, 
# MAGIC   annual_inc, 
# MAGIC   dti,
# MAGIC   addr_state,
# MAGIC   term,
# MAGIC   home_ownership, 
# MAGIC   purpose, 
# MAGIC   application_type, 
# MAGIC   delinq_2yrs, 
# MAGIC   total_acc,
# MAGIC   case
# MAGIC     when loan_status = "Current" then "false"
# MAGIC     when loan_status = "Fully Paid" then "false"
# MAGIC     else "true"
# MAGIC   end as bad_loan
# MAGIC from 
# MAGIC   tmp_loan_club

# COMMAND ----------

# make sql string
sql_stmt = """
select
  loan_status, 
  cast(regexp_replace(int_rate, '%', '') as float) as int_rate,
  cast(regexp_replace(revol_util, '%', '') as float) as revol_util,
  cast(substring(issue_d, 5, 4) as double) as issue_year,
  cast(substring(earliest_cr_line, 5, 4) as double) as earliest_year,
  cast(substring(issue_d, 5, 4) as double) -
  cast(substring(earliest_cr_line, 5, 4) as double) as credit_length_in_years,
  cast(regexp_replace(regexp_replace(regexp_replace(emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", ""), "< 1", "0"), "10\\+", "10") as float) as emp_length,
  verification_status, 
  total_pymnt,
  loan_amnt, 
  grade, 
  annual_inc, 
  dti,
  addr_state,
  term,
  home_ownership, 
  purpose, 
  application_type, 
  delinq_2yrs, 
  total_acc,
  case
    when loan_status = "Current" then "false"
    when loan_status = "Fully Paid" then "false"
    else "true"
  end as bad_loan
from 
  tmp_loan_club
"""

# execute
df = spark.sql(sql_stmt)

# COMMAND ----------

# Write out csv file
path = "/lake/bronze/loan/temp"
(
  df.repartition(1).write
    .format("parquet")
    .mode("overwrite")
    .save(path)
)

# COMMAND ----------

# create single file
unwanted_file_cleanup("/lake/bronze/loan/temp/", "/lake/bronze/loan/club-data.parquet", "parquet")

# COMMAND ----------

dbutils.fs.ls("/lake/bronze/loan/")

# COMMAND ----------


