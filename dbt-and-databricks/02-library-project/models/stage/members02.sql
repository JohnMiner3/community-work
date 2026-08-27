{{ config(
    materialized='view',
    schema='stage'
) }}
select *
from {{ ref('members01') }}
