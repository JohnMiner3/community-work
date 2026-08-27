{% snapshot MediaType02 %}

{{ config(
    strategy='check',
    unique_key='`MediaTypeId`',
    check_cols=['`Name`']
) }}

select * from {{ ref('MediaType') }}

{% endsnapshot %}
