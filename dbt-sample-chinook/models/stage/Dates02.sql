{{
  config(
    materialized='table',
    alias='Dates02',
    as_columnstore=False
  )
}}

WITH date_spine AS (
  {{
    dbt_utils.date_spine(
      start_date="CAST('2015-01-01' AS DATE)", 
      end_date="DATEADD(YEAR, 30, CAST('2015-01-01' AS DATE))",
      datepart="day")
  }}
),

-- Add other date attributes
datedetails AS (
  SELECT
    - DATEDIFF(DAY, date_day, '2015-01-01') as DateId,
    CAST(date_day AS DATETIME2) as DateVal,
    YEAR(date_day) AS CalendarYear,
    MONTH(date_day) AS CalendarMonth,
    DAY(date_day) AS CalendarDay 
  FROM
    date_spine
)

SELECT
  *
FROM
  datedetails
