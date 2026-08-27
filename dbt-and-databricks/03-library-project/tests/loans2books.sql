select 
 *
from 
  {{ ref('loans01') }} as a
where 
  a."BookID" not in
  (
	  select distinct COALESCE("BookID", -1) from {{ ref('books01') }}
  )