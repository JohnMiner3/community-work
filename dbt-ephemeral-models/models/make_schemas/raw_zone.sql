{{ config(materialized='ephemeral') }}

--
--  Create the raw schema if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data_raw')
  BEGIN
    EXEC('CREATE SCHEMA data_raw AUTHORIZATION dbo;');
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}