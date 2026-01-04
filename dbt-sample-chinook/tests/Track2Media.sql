select 
 *
from 
  {{ ref('Track02') }} as a
where 
  a.MediaTypeId not in
  (
	  select distinct MediaTypeId from {{ ref('MediaType02') }}
  )