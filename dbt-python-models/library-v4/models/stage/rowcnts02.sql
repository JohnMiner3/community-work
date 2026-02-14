-- not supported in dbt adapter
{{ 
  config(materialized='materialized_view') 
}}
SELECT 'books' as table_name, count(*) as row_count
FROM {{ ref('books02') }}
UNION ALL
SELECT 'members' as table_name, count(*) as row_count
FROM {{ ref('dates02') }}
UNION ALL
SELECT 'dates' as table_name, count(*) as row_count 
FROM {{ ref('loans02') }}
UNION ALL
SELECT 'dates' as table_name, count(*) as row_count 
FROM {{ ref('members02') }}

