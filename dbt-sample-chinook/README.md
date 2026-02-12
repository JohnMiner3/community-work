# Open Source Project

Welcome to the **Chinook Sample dbt Project** created by John Miner.

Special thanks to <span style="color:red">Luis Rocha</span> for creating the SQL scripts for various databases.
Please see copy right information for this original work here.

https://github.com/lerocha/chinook-database?tab=License-1-ov-file


# Configuring dbt for SQL Server

1 - Download the project to your local machine.

2 - Make sure you have visual studio code installed as well as a latest version of python.

3 - Make sure dbt core and the adapter for SQL Server are installed.

python -m pip install dbt-core dbt-sqlserver

4 - Make sure the latest ODBC driver for SQL Server is installed.

https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17


5 - Create a sample database named ChinookDbms.

6 - Create a login and user that has db_owner access to ChinookDbms database.  This example uses standard security but other authentication methods do exist.

7 - Modify the profiles.yml file to include the correct server, database, user and password.


# Deploy the sample database

1 - Check connectivity with the following command.

dbt debug

2 - Create the tables in the data_raw schema.

dbt seed

3 - Create the views and/or table in data_stage schema.

dbt run -s stage


4 - Create the SCD2 tables in the data_snap schema.

dbt snapshot

5 - Create the incremental load tables in the data_loads schema.

dbt run -s loads

6 - Create the dimension model in the data_marts schema.

dbt run -s marts

7 - Create the reporing quries in the data_analytics schema.

dbt run -s analytics


# Database Objects

1 - data_raw.[*] ~ the landing zone of the extract and load process

    - Album
    - Artist
    - Customer
    - Employee
    - Genre
    - Invoice
    - InvoiceLine
    - MediaType
    - Playlist
    - PlaylistTrack
    - Track

2 - data_stage.[*] ~ formatting of the raw data

    - Album01
    - Artist01
    - Customer01
    - Dates01 - example of ephermal table
    - Dates02 - use date_spine function to create date dimension
    - Employee01 - cast strings to dates
    - Invoice01 - cast strings to dates
    - InvoiceLine01 - add invoice date for incremental load
    - Playlist01 
    - PlaylistTrack01
    - Track01 - fix null composers

3 - data_snap.[*] ~ slowly changing dimensions type 2

    - Album02
    - Artist02
    - Customer02
    - Employee02
    - Genre02
    - MediaType02
    - Playlist02
    - PlaylistTrack02
    - Track02

4 - data_loads.[*] ~ incemental load of transactional data

    - Invoice02
    - InvoiceLine02

5 - data_marts.[*] ~ the dimensional model

    - DimCustomer
    - DimDate
    - DimEmployees
    - DimLists
    - DimProducts
    - FactSales


6 - data_analytics.[*] ~ published views for reporting

    - SalesByCountry


# Documentation + Tests

1 - adding properties to the schema

* **seed_proprties.yml**
    - data tests for primary keys on all tables
    - documentation of data_raw tables

* **stage_schema.yml**
    - documentation of data_stage views + tables
    - data tests for primary keys on Dates02 table.

* **loads_schema.yml**
    - documentation of data_mart tables

* **marts_schema.yml**
    - documentation of data_mart tables

* **analytics_schema.yml**
    - documentation of data_analytics views

2 - custom [./tests] subfolder

Referential Integrity Tests

    - Album2Artist
    - Customer2Employee
    - Invoice2Customer
    - Line2Invoice
    - Line2Track
    - PlaylistTrack2Playlist
    - PlaylistTrack2Track
    - Track2Album
    - Track2Genre
    - Track2Media

Custom Business Logic

    - SmallSales


3 - display all tests using this cmd

    dbt ls --resource-type test


# Backlog

 + 1.a - add effective dating to DimLists + DimProducts
 + 1.b - add a couple more reporting views
 + 1.c - might want to move Dates02 to pre-stage
 + 1.d - use date key in invoices, invoice lines + employee
 + 1.e - convert place holder for DateDim to real date table


# Revision History

 1.00 - 2 Jan 2026 
 
    - initial release

 1.01 - 4 Jan 2026
    - complete readme file
    - add mention for Chinook licensing
    - add custom tests for integrity
    - create backlog section

# Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news
