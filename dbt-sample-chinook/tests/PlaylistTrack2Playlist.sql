select 
 *
from 
  {{ ref('PlaylistTrack02') }} as a
where 
  a.PlaylistId not in
  (
	  select distinct PlaylistId from {{ ref('Playlist02') }}
  )