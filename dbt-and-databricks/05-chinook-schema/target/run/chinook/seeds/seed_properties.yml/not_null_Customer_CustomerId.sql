
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerId
from "chinook01"."data_raw"."Customer01"
where CustomerId is null



  
  
      
    ) dbt_internal_test