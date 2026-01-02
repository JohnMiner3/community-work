{% snapshot Track02 %}

{{ config(
    target_schema='data_snap',
    strategy='check',
    unique_key='TrackId',
    check_cols=['Name','AlbumId','MediaTypeId','GenreId','Composer','Milliseconds','Bytes','UnitPrice']
) }}

select * from {{ ref('Track01') }}

{% endsnapshot %}