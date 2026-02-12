{{ config(materialized='ephemeral') }}

--
--  Create the library database if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'library')
  BEGIN
    CREATE DATABASE [library] ( EDITION = 'GeneralPurpose', SERVICE_OBJECTIVE = 'GP_S_Gen5_4' ) ;
  END;
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

