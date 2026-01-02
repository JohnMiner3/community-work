select 
	inv.*,
	lin.InvoiceLineId,
	lin.TrackId,
	lin.UnitPrice,
	lin.Quantity
from 
	{{ ref('Invoice02') }}  as inv
join 
	{{ ref('InvoiceLine02') }} as lin
on 
	inv.InvoiceId = lin.InvoiceId