{% snapshot Artist02 %}

{{ config(
    strategy='check',
    unique_key='"ArtistId"',
    check_cols=['"Name"']
) }}

select * from {{ ref('Artist') }}

{% endsnapshot %}
