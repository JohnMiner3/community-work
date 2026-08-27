select 
	inv.*,
	lin.`InvoiceLineId` as `InvoiceLineId`,
	lin.`TrackId` as `TrackId`,
	lin.`UnitPrice` as `UnitPrice`,
	lin.`Quantity` as `Quantity`
from 
	`uc_sql_server_central`.`data_stage`.`invoice02`  as inv
join 
	`uc_sql_server_central`.`data_stage`.`invoiceline02` as lin
on 
	inv.`InvoiceId` = lin.`InvoiceId`