# Open Source Project

Welcome to the **Python Models Project** created by John Miner.

The purpose of this project is to show how dSpark SQL can be executed against
an Azure Databricks SQL Warehouse and Python can be executed against an Azure 
Databricks Spark Cluster.


# Configuring dbt for Databricks

1 - Download the project to your local machine.

2 - Make sure you have visual studio code installed as well as a latest version of python.

3 - Make sure dbt core and the adapter for databricks are installed.

python -m pip install dbt-core dbt-databricks


4 - Modify the profiles.yml file to include the correct host, http path, and access token.

Use the warehouse for all SQL Models and the Cluster for all Python Models.



# Deploy the library schema (SQL model)

1 - Check connectivity to the warehouse with the following command.

dbt debug 

2 - Manually create a new unity catalog called uc_sql_server_central


3 - Create the raw tables by seeding them.

dbt seed


4 - Create the snapshots in the new dastabase

dbt snapshots


5 - Create the tables in the data_stage schema.

dbt run -s stage


6 - Create the tables in the data_mart schema.

dbt run -s mart


# Library schema 

1 - data_raw.[*] ~ the landing zone of the extract and load process

    - Books01
    - Loans01
    - Members01
	- Dates01
	
2 - data_snapshot.[*] ~ slowly changing dimensions type 2

    - Books02
    - Members02
	- Dates02
	
3 - data_stage.[*] ~ incremental load of transactional data

    - Loans02
	- Loans03 ~ test views
	- rowcnt02 ~ test materialized views


# Deploy the address schema (Python model)

1 - Check connectivity to the cluster with the following command.

dbt debug 

2 - Re-use the new unity catalog called uc_sql_server_central


3 - Create the one raw table by seeding them.

dbt seed

4 - Create the one stage table by using a python model.

dbt run -s stage


# Address schema 

1 - data_bronze.[*] ~ the landing zone

    - Address01
	
2 - data_silver.[*] ~ call Azure Maps API

    - Address02
	

# Backlog

 + nothing at this time


# Revision History

 1.00 - 12 Feb 2026 
 
    - initial release


# Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news
