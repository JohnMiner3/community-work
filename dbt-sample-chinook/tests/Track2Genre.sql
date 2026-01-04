select 
 *
from 
  {{ ref('Track02') }} as a
where 
  a.GenreId not in
  (
	  select distinct GenreId from {{ ref('Genre02') }}
  )