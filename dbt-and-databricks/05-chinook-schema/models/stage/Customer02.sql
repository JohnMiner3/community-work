{{
  config(
    materialized='incremental',
    unique_key='"CustomerId"',
    incremental_strategy='merge'
  )
}}

select
    *
from {{ ref('Customer') }}
