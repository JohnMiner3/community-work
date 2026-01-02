select 
 *
from 
  {{ ref('Album02') }} as a
where 
  a.ArtistId not in
  (
	  select distinct ArtistId from {{ ref('Artist02') }}
  )