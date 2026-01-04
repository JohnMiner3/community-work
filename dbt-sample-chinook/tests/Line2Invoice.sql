select 
 *
from 
  {{ ref('InvoiceLine02') }} as a
where 
  a.InvoiceId not in
  (
	  select distinct InvoiceId from {{ ref('Invoice02') }}
  )