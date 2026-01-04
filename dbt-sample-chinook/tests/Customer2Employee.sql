select 
 *
from 
  {{ ref('Customer02') }} as a
where 
  a.SupportRepId not in
  (
	  select distinct EmployeeId from {{ ref('Employee02') }}
  )