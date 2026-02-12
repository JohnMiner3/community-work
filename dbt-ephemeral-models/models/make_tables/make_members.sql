{{ config(materialized='ephemeral') }}

--
--  Create the members01 table if it does not exist
--

-- set cmd to create schema if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.objects  WHERE object_id = OBJECT_ID(N'[data_raw].[members01]') AND type in (N'U'))
  BEGIN
    CREATE TABLE [data_raw].[members01](
      [MemberID] [int] NULL,
      [Name] [varchar](17) NULL,
      [Email] [varchar](27) NULL,
      [JoinDate] [date] NULL,
      [Status] [varchar](16) NULL
    );
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

