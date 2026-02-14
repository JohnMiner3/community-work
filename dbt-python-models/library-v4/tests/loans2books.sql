select 
 *
from 
  {{ ref('loans01') }} as a
where 
  a.BookID not in
  (
	  select distinct BookID from {{ ref('books01') }}
  )