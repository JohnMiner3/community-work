{{ config(materialized='ephemeral') }}

--
--  Give user select permissions on the data_mart schema
--

-- use database role irl.
{% set query %}

    -- create user
    IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'svcacct01')
    BEGIN
        CREATE USER [svcacct01] FOR LOGIN [svcacct01];
    END;

    -- create role
    IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'mart_readonly' AND type = 'R')
    BEGIN
        CREATE ROLE [mart_readonly];
    END;

    -- add user to role
    EXECUTE sp_addrolemember 'mart_readonly', 'svcacct01';

    -- give role rights to schema
    GRANT SELECT ON SCHEMA :: [data_mart] TO [mart_readonly];

{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}
