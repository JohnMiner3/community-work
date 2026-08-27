
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EmployeeId
from "chinook01"."data_raw"."Employee01"
where EmployeeId is null



  
  
      
    ) dbt_internal_test