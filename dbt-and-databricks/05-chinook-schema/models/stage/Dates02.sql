{{ config(materialized='table') }}

WITH date_spine AS (
  {{
    dbt_utils.date_spine(
      start_date="CAST('2015-01-01' AS DATE)", 
      end_date="CAST('2045-01-01' AS DATE)",
      datepart="day")
  }}
),

-- Add other date attributes
date_details AS (
  SELECT
    {{ dbt.datediff("date_day", "'2015-01-01'", "day") }} AS DateId,
    cast("date_day" as {{ dbt.type_timestamp() }}) AS DateValue,
    {{ dbt_date.date_part('year', 'date_day') }} AS CalendarYear,
    {{ dbt_date.date_part('month', 'date_day') }} AS CalendarMonth,
    {{ dbt_date.date_part('day', 'date_day') }} AS CalendarDay
  FROM
    date_spine
)

SELECT
  *
FROM
  date_details