
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select InvoiceId
from "chinook01"."data_raw"."Invoice01"
where InvoiceId is null



  
  
      
    ) dbt_internal_test