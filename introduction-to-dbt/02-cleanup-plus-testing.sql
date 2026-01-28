--
--  Test snaphots (SCD Type 2)
--

-- show data (raw)
select * from [data_raw].[members01] where MemberID = 1;

-- update record
update m
set [JoinDate] = getdate(), [Status] = 'Active'
from [data_raw].[members01] as m 
where MemberID = 1;

-- show data (snapshot)
select * from [data_snapshot].[members03] where MemberID = 1;


--
--  Test incremental load
--

-- show existing date table
select top 5 * from [data_raw].[dates01] order by id desc;

-- add new record
insert into [data_raw].[dates01]
select
    1462 as id, 
    '2027-01-01' as date,
    2027 as year,
    1 as quarter,
    1 as month,
    'January' as month_name,
    1 as day,
    4 as day_of_week,
    'Friday' as day_name,
    1 as week_of_year,
    0 as is_weekend
;


-- show existing date table
select top 5 * from [data_stage].[dates02] order by id desc;



--
--  Drop user defined tables
--

DECLARE @sql VARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(t.name) + ';' + CHAR(13) + CHAR(10)
FROM sys.tables t where t.is_ms_shipped = 0;
EXEC(@sql);
GO


--
--  Drop user defined views
--

DECLARE @sql VARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP VIEW ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(v.name) + ';' + CHAR(13) + CHAR(10)
FROM sys.views v where v.is_ms_shipped = 0;
EXEC(@sql);
GO