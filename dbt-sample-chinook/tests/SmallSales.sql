select 
 *
from 
  {{ ref('FactSales') }} as a
where 
  a.Total < 2