{{ config(materialized='view') }}

select
    *
from {{ ref('loans01') }}
