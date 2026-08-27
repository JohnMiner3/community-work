{% snapshot PlaylistTrack02 %}

{{ config(
    strategy='check',
    unique_key='`skey`',
    check_cols=['`PlaylistId`', '`TrackId`']
) }}

select 
  `PlaylistId`,
  `TrackId`,
  `skey`
from 
(
  select 
    `PlaylistId`,
    `TrackId`,
    {{ dbt_utils.generate_surrogate_key(['`PlaylistId`', '`TrackId`']) }} as `skey`
    from {{ ref('PlaylistTrack') }}
) as d

{% endsnapshot %}
