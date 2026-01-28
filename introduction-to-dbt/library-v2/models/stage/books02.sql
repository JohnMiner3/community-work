{{ config(
    materialized='table',
    schema='stage'
) }}
select *
from {{ ref('books01') }}
