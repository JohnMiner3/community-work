
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select InvoiceLineId
from "chinook01"."data_raw"."InvoiceLine01"
where InvoiceLineId is null



  
  
      
    ) dbt_internal_test