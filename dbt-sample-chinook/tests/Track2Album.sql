select 
 *
from 
  {{ ref('Track02') }} as a
where 
  a.AlbumId not in
  (
	  select distinct AlbumId from {{ ref('Album02') }}
  )