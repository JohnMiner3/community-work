select 
	inv.*,
	lin."InvoiceLineId" as "InvoiceLineId",
	lin."TrackId" as "TrackId",
	lin."UnitPrice" as "UnitPrice",
	lin."Quantity" as "Quantity"
from 
	{{ ref('Invoice02') }}  as inv
join 
	{{ ref('InvoiceLine02') }} as lin
on 
	inv."InvoiceId" = lin."InvoiceId"