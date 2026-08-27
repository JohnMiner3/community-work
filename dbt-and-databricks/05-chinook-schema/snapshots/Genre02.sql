{% snapshot Genre02 %}

{{ config(
    strategy='check',
    unique_key='"GenreId"',
    check_cols=['"Name"']
) }}

select * from {{ ref('Genre') }}

{% endsnapshot %}
