{% snapshot dates02 %}

{{ config(
    strategy='check',
    unique_key='id',
    check_cols=['date', 'year', 'quarter', 'month', 'month_name', 'day', 'day_of_week', 'day_name', 'week_of_year', 'is_weekend']
) }}

select * from {{ ref('dates01') }}

{% endsnapshot %}
