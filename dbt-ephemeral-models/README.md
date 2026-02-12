# Open Source Project

Welcome to the **Ephemeral Models Project** created by John Miner.

The purpose of this project is to show how dynamic T-SQL can be executed against
an Azure SQL database.


# Configuring dbt for SQL Server

1 - Download the project to your local machine.

2 - Make sure you have visual studio code installed as well as a latest version of python.

3 - Make sure dbt core and the adapter for SQL Server are installed.

python -m pip install dbt-core dbt-sqlserver

4 - Make sure the latest ODBC driver for SQL Server is installed.

https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17


5 - Modify the profiles.yml file to include the correct server, database, user and password.

The master section should have a user that can create a database.

The library section should have a user that has db_owner rights to the library databases.

The same account for testing can be used for both.  



# Deploy the sample database

1 - Check connectivity to the master database with the following command.

dbt debug --profile master


2 - Create the new database named library.

dbt run --profile master -s make_database


3 - Check connectivity to the library database with the following command.

dbt debug --profile library


4 - Create the schemas in the new database.

dbt run --profile library -s make_schemas


5 - Create the tables in the data_raw schema.

dbt run --profile library -s make_tables


6 - Add comments to the tables in the data_raw schema.

dbt run --profile library -s add_comments


7 - Add security to the data_raw schema.

dbt run --profile library -s add_security

* assumes login svcacct01 exists on the server


8 - Tear down objects (views, tables, schemas).

dbt run --profile library -s clean_database

* might fail due to order, run by file if needed


# Database Objects

1 - data_raw.[*] ~ the landing zone of the extract and load process

    - Books
    - Loans
    - Members

# Backlog

 + nothing at this time


# Revision History

 1.00 - 12 Feb 2026 
 
    - initial release


# Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news
