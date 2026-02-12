{{ config(materialized='ephemeral') }}

--
--  Add description to the loans01 table
--

-- set cmd to create schema if not exists
{% set query %}
  EXEC sp_addextendedproperty 
      @name = N'MS_Description', 
      @value = N'This table contains information about library books on loan, including their ID, book ID, member ID, borrow date, due date, and return date.', 
      @level0type = N'SCHEMA', 
      @level0name = N'data_raw', 
      @level1type = N'TABLE', 
      @level1name = N'loans01';
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}


