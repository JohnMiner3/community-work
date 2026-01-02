select
  year(s.InvoiceDate) as SalesYear,
  month(s.InvoiceDate) as SalesMonth,
  e.FirstName + ' '  + e.LastName as SalesPerson,
  sum(s.Total) as MonthlySales
from 
  {{ ref('FactSales') }} as s
join
  {{ ref('DimCustomers') }} as c
on 
  s.CustomerId = c.CustomerId
join 
  {{ ref('DimEmployees') }} as e
on 
  c.SupportRepId = e.EmployeeId
group by
  year(s.InvoiceDate),
  month(s.InvoiceDate),
  e.FirstName + ' '  + e.LastName
order by
  SalesPerson, 
  SalesYear, 
  SalesMonth