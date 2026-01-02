{{ 
  config(materialized='ephemeral') 
}}
SELECT 
  1 as DateId, 
  dateadd(DAY, 0, cast('20150101' as date)) as DateVal  
