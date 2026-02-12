{{ config(materialized='ephemeral') }}

--
--  Drop user defined tables
--

-- create dynamic code
{% set query %}

  DECLARE @sql VARCHAR(MAX) = '';
  SELECT @sql = @sql + 'DROP SCHEMA ' + QUOTENAME(SCHEMA_NAME(schema_id)) +  ';' + CHAR(13) + CHAR(10)
  FROM sys.schemas s where s.schema_id > 4 and s.schema_id < 16384
  EXEC(@sql);

{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

