
select 
  InvoiceId,
  CustomerId,
  {{ cast_string_to_datetime2('InvoiceDate', 111) }} as InvoiceDate,
  BillingAddress,
  BillingCity,
  BillingState,
  BillingCountry,
  BillingPostalCode,
  Total
from {{ source('Chinook', 'Invoice') }}