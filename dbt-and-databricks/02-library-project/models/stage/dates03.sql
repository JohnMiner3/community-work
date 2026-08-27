{{ 
  config(materialized='ephemeral') 
}}
SELECT 
    1 as id, 
    '1/1/2023' as date,
    2023 as year,
    1 as quarter,
    1 as month,
    'January' as month_name,
    1 as day,
    6 as day_of_week,
    'Sunday' as day_name,
    52 as week_of_year,
    1 as is_weekend

