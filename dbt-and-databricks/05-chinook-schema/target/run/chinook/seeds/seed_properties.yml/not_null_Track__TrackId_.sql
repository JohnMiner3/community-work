
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "TrackId"
from "chinook01"."data_raw"."Track01"
where "TrackId" is null



  
  
      
    ) dbt_internal_test