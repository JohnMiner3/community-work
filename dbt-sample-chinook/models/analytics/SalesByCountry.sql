select 
	year(InvoiceDate) * 100 + month(InvoiceDate) as YearMonth,
	BillingCountry,
	COUNT(DISTINCT CustomerId) as Customers,
	COUNT(DISTINCT InvoiceId) as Invoices,
	COUNT(TrackId) as Tracks,
	SUM(Quantity) as TotalQty,
	SUM(UnitPrice * Quantity) as TotalSales
from 
  [data_marts].[FactSales]
group by
	year(InvoiceDate) * 100 + month(InvoiceDate),
	BillingCountry
