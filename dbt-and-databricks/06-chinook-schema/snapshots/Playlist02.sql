{% snapshot Playlist02 %}

{{ config(
    strategy='check',
    unique_key='`PlaylistId`',
    check_cols=['`Name`']
) }}

select * from {{ ref('Playlist') }}

{% endsnapshot %}
