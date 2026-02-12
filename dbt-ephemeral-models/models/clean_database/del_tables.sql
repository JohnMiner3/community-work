{{ config(materialized='ephemeral') }}

--
--  Drop user defined tables
--

-- create dynamic code
{% set query %}

  DECLARE @sql VARCHAR(MAX) = '';
  SELECT @sql = @sql + 'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(t.name) + ';' + CHAR(13) + CHAR(10)
  FROM sys.tables t where t.is_ms_shipped = 0;
  EXEC(@sql);

{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

