# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         03-refining-lending-club-data
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-15-2020
# MAGIC     Purpose:  Demostrate how to refine data.
# MAGIC     
# MAGIC Assumptions:
# MAGIC     None
# MAGIC     
# MAGIC Algorithm:
# MAGIC     1 - Using dataframes to manipulate data.
# MAGIC     2 - Using spark sql to manipulate data.   
# MAGIC     
# MAGIC ```

# COMMAND ----------

#
# Show data file & readme file
#


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/lending-club-loan-stats/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/databricks-datasets/lending-club-loan-stats/lendingClubData.txt

# COMMAND ----------

#
#  Continue with sample datalake processing
#

dbutils.fs.mkdirs("/rissug/bronze/lending_club")
dbutils.fs.mkdirs("/rissug/silver/lending_club")   

dbutils.fs.cp("/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv", "/rissug/bronze/lending_club/loan_stats_2018q2.csv")


# COMMAND ----------

# MAGIC %sh
# MAGIC # ls /dbfs/rissug/silver/lending_club/process01.parquet
# MAGIC # rm -r /dbfs/rissug/silver/lending_club/process01.parquet
# MAGIC # rm -r /dbfs/rissug/silver/lending_club/process02.parquet

# COMMAND ----------

#
#  Read in unprocessed data
#

# Read in file
df_raw = spark.read.csv("/rissug/bronze/lending_club/loan_stats_2018q2.csv", header=True)

# Choose subset of data
loans1 = df_raw.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", "application_type", "delinq_2yrs", "total_acc")

# Show the data
display(loans1)

# COMMAND ----------

#
#  Process data via dataframe cmds
#

# Include functions
from pyspark.sql.functions import *

# Create bad loan flag
loans1 = loans1.withColumn("bad_loan", (~loans1.loan_status.isin(["Current", "Fully Paid"])).cast("string"))

# Convert to numerics
loans1 = loans1.withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float')) \
             .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float')) \
             .withColumn('issue_year',  substring(loans1.issue_d, 5, 4).cast('double') ) \
             .withColumn('earliest_year', substring(loans1.earliest_cr_line, 5, 4).cast('double'))

# Calculate len in yrs
loans1 = loans1.withColumn('credit_length_in_years', (loans1.issue_year - loans1.earliest_year))

# Use regex to clean up 
loans1 = loans1.withColumn('emp_length', trim(regexp_replace(loans1.emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", "") ))
loans1 = loans1.withColumn('emp_length', trim(regexp_replace(loans1.emp_length, "< 1", "0") ))
loans1 = loans1.withColumn('emp_length', trim(regexp_replace(loans1.emp_length, "10\\+", "10") ).cast('float'))

# Show the data
display(loans1)

# Write out data
loans1.repartition(2).write.parquet('/rissug/silver/lending_club/process01.parquet')


# COMMAND ----------

#
#  Create loan club table 01
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.loan_club_process_01")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.loan_club_process_01
    USING PARQUET
    LOCATION '/rissug/silver/lending_club/process01.parquet'
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE nyc.loan_club_process_02

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Get total count
# MAGIC select count(*) from nyc.loan_club_process_01
# MAGIC 
# MAGIC -- Show 10 records
# MAGIC -- select * from nyc.loan_club_process_01 limit 10

# COMMAND ----------

#
# Create session level view
#

df_raw.createOrReplaceTempView("tmp_loan_club")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Get total count
# MAGIC -- select count(*) from tmp_loan_club
# MAGIC 
# MAGIC -- Get total count
# MAGIC select * from tmp_loan_club

# COMMAND ----------

#
#  Process data via sql
#

# Multi line hear doc
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

# Run sql & retrieve results
loans2 = spark.sql(sql_stmt)

# Write out data
loans2.repartition(2).write.parquet('/rissug/silver/lending_club/process02.parquet')


# COMMAND ----------

#
#  Create loan club table 02
#

# Try to delete table
try:
  df = spark.sql("DROP TABLE nyc.loan_club_process_02")
except:
  pass

# Try to create table
sql_stmt = """
  CREATE TABLE nyc.loan_club_process_02
    USING PARQUET
    LOCATION '/rissug/silver/lending_club/process02.parquet'
"""
df = spark.sql(sql_stmt)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Get total count
# MAGIC -- select count(*) from nyc.loan_club_process_02
# MAGIC 
# MAGIC -- Show 10 records
# MAGIC select * from nyc.loan_club_process_02 limit 10

# COMMAND ----------


