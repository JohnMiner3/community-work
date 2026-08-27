/******************************************************
 *
 * Name:         0101-pg-manage-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     09-01-2026
 *     Purpose:  Create databases for the presentation.
 * 
 ******************************************************/

--
--  Show databases
-- 

SELECT datname FROM pg_database;

--
--  Show schemas
-- 

SELECT schema_name 
FROM information_schema.schemata;


--
--  Create database 1
-- 

create database library01;


--
--  Create database 2
-- 

create database chinook01;


--
--  Create login
-- 

CREATE ROLE dbt_svc_acct LOGIN PASSWORD '';


--
--  Grant connect
-- 

GRANT CONNECT ON DATABASE library01 TO dbt_svc_acct;
GRANT CONNECT ON DATABASE chinook01 TO dbt_svc_acct;



--
--  Grant all privileges
-- 

GRANT ALL PRIVILEGES ON DATABASE library01 TO dbt_svc_acct;
GRANT ALL PRIVILEGES ON DATABASE chinook01 TO dbt_svc_acct;


--
--  Drop schema - library or chinook model
-- 

DROP SCHEMA "data_snapshot" CASCADE;
DROP SCHEMA "data_raw" CASCADE;
DROP SCHEMA "data_mart" CASCADE;
DROP SCHEMA "data_stage" CASCADE;




