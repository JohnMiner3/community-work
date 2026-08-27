{% snapshot Album02 %}

{{ config(
    strategy='check',
    unique_key='`AlbumId`',
    check_cols=['`Title`', '`ArtistId`']
) }}

select * from {{ ref('Album') }}

{% endsnapshot %}
