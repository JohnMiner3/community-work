{{ config(materialized='ephemeral') }}

--
--  Create the stage schema if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data_stage')
  BEGIN
    EXEC('CREATE SCHEMA data_stage AUTHORIZATION dbo;');
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}