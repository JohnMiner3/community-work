{{ config(materialized='ephemeral') }}

--
--  Create the loans01 table if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.objects  WHERE object_id = OBJECT_ID(N'[data_raw].[loans01]') AND type in (N'U'))
  BEGIN
    CREATE TABLE [data_raw].[loans01](
      [LoanID] [int] NULL,
      [BookID] [int] NULL,
      [MemberID] [int] NULL,
      [BorrowDate] [date] NULL,
      [DueDate] [date] NULL,
      [ReturnDate] [date] NULL
    );
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

