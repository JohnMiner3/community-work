{{
  config(
    materialized='incremental',
    unique_key='"InvoiceId"',
    incremental_strategy='merge'
  )
}}

select
    *
from {{ ref('Invoice') }}
