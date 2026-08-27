{% snapshot Track02 %}

{{ config(
    strategy='check',
    unique_key='"TrackId"',
    check_cols=['"Name"', '"AlbumId"', '"MediaTypeId"', '"GenreId"', '"Composer"', '"Milliseconds"', '"Bytes"', '"UnitPrice"']
) }}

select * from {{ ref('Track') }}

{% endsnapshot %}
