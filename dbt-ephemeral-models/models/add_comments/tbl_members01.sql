{{ config(materialized='ephemeral') }}

--
--  Add description to the members01 table
--

-- set cmd to create schema if not exists
{% set query %}
  EXEC sp_addextendedproperty 
      @name = N'MS_Description', 
      @value = N'This table contains information about library members, including their ID, name, email, join date, and status.', 
      @level0type = N'SCHEMA', 
      @level0name = N'data_raw', 
      @level1type = N'TABLE', 
      @level1name = N'members01';
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}


