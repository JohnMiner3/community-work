 /******************************************************
  *
  * Name:         show-record-counts.sql
  *     
  * Design Phase:
  *     Author:   John Miner
  *     Date:     04-01-2020
  *     Purpose:  Examine the slot events schema for trends.
  * 
  ******************************************************/

-- Get table counts
SELECT
	QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name) AS [TableName],
    SUM(p.row_count) AS [RowCount]
FROM
    sys.objects AS o
INNER JOIN 
	sys.dm_db_partition_stats AS p
ON 
	o.object_id = p.object_id
WHERE 
    o.type = 'U' AND 
	o.is_ms_shipped = 0x0 AND 
	p.index_id < 2
GROUP BY
	o.schema_id,
    o.name
ORDER BY 
	[TableName]
GO


--
-- watermark, days, hours, recs
--

select 'total days' as key1, cast(count(*) as varchar(21) ) as val1 
from
(
  select distinct cast(event_dt as date) as val1 
  from [active].[slot_events]
) d
  union
select 'total hours' as key1, cast(count(*) / 45 / 3397 as varchar(21) ) as val1 
from [active].[slot_events]
  union
select 'total recs' as key1, cast(count(*) as varchar(21) ) as val1 
from [active].[slot_events]
  union
select 'water mark' as key1, convert(char(21), process_date_hour, 121) as val1 
from [stage].[current_watermark]


--
-- day and hour vs running total
--

-- drop temp table
drop table if exists #temp1

-- running total of records
select 
	event_dt, 
	count(*) as total_by_tz, 
	sum(count(*)) OVER (ORDER BY event_dt) as grand_total
into 
	#temp1
from 
	[active].[slot_events]
group by 
	event_dt

-- not expected total
select * from #temp1 
where total_by_tz <> 152865

