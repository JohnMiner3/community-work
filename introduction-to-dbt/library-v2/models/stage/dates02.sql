{{
    config(
        materialized='incremental'
    )
}}

select *
from {{ ref('dates01') }}

{% if is_incremental() %}
where id >= (select max(id) from {{ this }} )
{% endif %}