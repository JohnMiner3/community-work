{{
  config(
    materialized='incremental',
    unique_key='LoanID',
    incremental_strategy='merge'
  )
}}

select
    *
from {{ ref('loans01') }}
