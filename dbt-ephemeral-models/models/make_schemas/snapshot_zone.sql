{{ config(materialized='ephemeral') }}

--
--  Create snapshot schema if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data_snapshot')
  BEGIN
    EXEC('CREATE SCHEMA data_snapshot AUTHORIZATION dbo;');
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}
