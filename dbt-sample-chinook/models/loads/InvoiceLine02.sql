{{
    config(
        materialized='incremental'
    )
}}

select *
from {{ ref('InvoiceLine01') }}

{% if is_incremental() %}
where InvoiceDate >= (select coalesce(max(InvoiceDate),'1900-01-01') from {{ this }} )
{% endif %}