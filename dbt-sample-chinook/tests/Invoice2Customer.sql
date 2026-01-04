select 
 *
from 
  {{ ref('Invoice02') }} as a
where 
  a.CustomerId not in
  (
	  select distinct CustomerId from {{ ref('Customer02') }}
  )