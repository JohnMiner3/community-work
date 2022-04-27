# Databricks notebook source
# MAGIC %md
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         5-use-tables-not-files
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-01-2020
# MAGIC     Purpose:  External hive tables
# MAGIC 
# MAGIC Learning Guide:
# MAGIC     1 - Create hive database
# MAGIC     2 - Create hive tables
# MAGIC     3 - Explore records
# MAGIC     4 - More advance sql example
# MAGIC     
# MAGIC ```

# COMMAND ----------



# COMMAND ----------

#
# 1 - Create new hive database
#

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS talks CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS talks

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /lake/bronze

# COMMAND ----------

#
# 2 - Create hive tables
#

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE talks.amazon_products
# MAGIC   USING PARQUET
# MAGIC   LOCATION '/lake/bronze/amazon/product-data.parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE talks.diamond_data 
# MAGIC   USING PARQUET
# MAGIC   LOCATION '/lake/bronze/diamonds/diamonds-data.parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE talks.loan_club
# MAGIC   USING PARQUET
# MAGIC   LOCATION '/lake/bronze/loan/club-data.parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE talks.power_plant
# MAGIC   USING PARQUET
# MAGIC   LOCATION '/lake/bronze/power/plant-data.parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE talks.weather_observations
# MAGIC   USING PARQUET
# MAGIC   LOCATION '/lake/bronze/weather/temperature-data.parquet'

# COMMAND ----------



# COMMAND ----------

#
# 2 - Explore tables
#

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from talks.amazon_products limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from talks.diamond_data limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from talks.loan_club limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from talks.power_plant limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from talks.weather_observations

# COMMAND ----------



# COMMAND ----------

#
# 3 - More advance SQL
#

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   addr_state,
# MAGIC   bad_loan,
# MAGIC   count(loan_amnt) as num_loans,
# MAGIC   round(avg(loan_amnt), 4) as avg_amount 
# MAGIC from 
# MAGIC   talks.loan_club
# MAGIC where
# MAGIC   addr_state in ('RI', 'CT', 'MA', 'NH', 'VT', 'ME')
# MAGIC group by
# MAGIC   addr_state,
# MAGIC   bad_loan
# MAGIC having
# MAGIC   count(loan_amnt) > 0
# MAGIC order by
# MAGIC   addr_state,
# MAGIC   bad_loan

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   addr_state,
# MAGIC   case
# MAGIC     when earliest_year < 50 then earliest_year + 2000
# MAGIC     else  earliest_year + 1900
# MAGIC   end as loan_year,
# MAGIC   loan_amnt
# MAGIC from
# MAGIC   talks.loan_club

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_loan_data
# MAGIC as
# MAGIC (
# MAGIC select
# MAGIC   addr_state,
# MAGIC   case
# MAGIC     when earliest_year < 50 then earliest_year + 2000
# MAGIC     else  earliest_year + 1900
# MAGIC   end as loan_year,
# MAGIC   loan_amnt
# MAGIC from
# MAGIC   nyc.loan_club_process_02
# MAGIC )
# MAGIC select
# MAGIC   row_number() over ( PARTITION BY (addr_state, loan_year) ORDER BY loan_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) loan_id ,
# MAGIC   addr_state,
# MAGIC   loan_year,
# MAGIC   loan_amnt,
# MAGIC   SUM(loan_amnt) over ( PARTITION BY (addr_state, loan_year) ORDER BY loan_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) run_amt
# MAGIC from
# MAGIC   cte_loan_data
# MAGIC where
# MAGIC   addr_state in ('RI', 'CT', 'MA') and loan_year > 2010
# MAGIC order by
# MAGIC   addr_state,
# MAGIC   loan_year,
# MAGIC   loan_id

# COMMAND ----------


