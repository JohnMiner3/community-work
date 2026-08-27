{{
  config(
    materialized='incremental',
    unique_key='"InvoiceLineId"',
    incremental_strategy='merge'
  )
}}

select
    *
from {{ ref('InvoiceLine') }}
