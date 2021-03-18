# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "/files/tables/avatar.jpg" width="100" height="100" />
# MAGIC  
# MAGIC ```
# MAGIC 
# MAGIC Name:         04-exploring-lending-club-data
# MAGIC 
# MAGIC Design Phase:
# MAGIC     Author:   John Miner
# MAGIC     Date:     12-15-2020
# MAGIC     Purpose:  Demostrate how to explore data.
# MAGIC     
# MAGIC Assumptions:
# MAGIC     None
# MAGIC  
# MAGIC     
# MAGIC ```

# COMMAND ----------

#
# Show hive tables
#


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from nyc

# COMMAND ----------

#
# Take 10 records
#


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE nyc.loan_club_process_02

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc.loan_club_process_02 limit 10

# COMMAND ----------

#
# Aggregation - North East States - Bad vs Good Loans + Avg Amount of loan
#


# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   addr_state,
# MAGIC   bad_loan,
# MAGIC   count(loan_amnt) as num_loans,
# MAGIC   round(avg(loan_amnt), 4) as avg_amount 
# MAGIC from 
# MAGIC   nyc.loan_club_process_02
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

#
#  Common table expression + windowing functions
#


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
# MAGIC   nyc.loan_club_process_02

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
# MAGIC   row_number() over ( PARTITION BY (addr_state) ORDER BY loan_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) loan_id ,
# MAGIC   addr_state,
# MAGIC   loan_year,
# MAGIC   loan_amnt,
# MAGIC   SUM(loan_amnt) over ( PARTITION BY (addr_state) ORDER BY loan_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) run_amt
# MAGIC from
# MAGIC   cte_loan_data
# MAGIC where
# MAGIC   addr_state in ('RI', 'CT', 'MA') and loan_year > 2010
# MAGIC order by
# MAGIC   addr_state,
# MAGIC   loan_id
# MAGIC   
