
select 
  LIN.*,
  {{ cast_string_to_datetime2('INV.InvoiceDate', 111) }} as InvoiceDate
from {{ source('Chinook', 'InvoiceLine') }} as LIN
join {{ source('Chinook', 'Invoice') }} as INV
  on LIN.InvoiceId = INV.InvoiceId