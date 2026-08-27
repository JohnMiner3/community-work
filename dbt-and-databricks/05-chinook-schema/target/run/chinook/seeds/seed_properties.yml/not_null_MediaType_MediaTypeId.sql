
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MediaTypeId
from "chinook01"."data_raw"."MediaType01"
where MediaTypeId is null



  
  
      
    ) dbt_internal_test