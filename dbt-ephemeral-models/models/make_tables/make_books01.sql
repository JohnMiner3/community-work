{{ config(materialized='ephemeral') }}

--
--  Create the books01 table if it does not exist
--

-- set cmd to create table if not exists
{% set query %}
  IF NOT EXISTS (SELECT * FROM sys.objects  WHERE object_id = OBJECT_ID(N'[data_raw].[books01]') AND type in (N'U'))
  BEGIN
    CREATE TABLE [data_raw].[books01]
    (
	    [BookID] [int] NULL,
	    [Title] [varchar](45) NULL,
	    [Author] [varchar](23) NULL,
	    [ISBN] [varchar](17) NULL,
	    [Genre] [varchar](16) NULL,
	    [Quantity] [int] NULL
    );
  END
{% endset %}

-- exec the command
{% do exec_tsql_cmd(query) %}

