select 
 *
from 
  {{ ref('InvoiceLine02') }} as a
where 
  a.TrackId not in
  (
	  select distinct TrackId from {{ ref('Track02') }}
  )