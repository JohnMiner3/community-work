
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "ArtistId"
from "chinook01"."data_raw"."Artist01"
where "ArtistId" is null



  
  
      
    ) dbt_internal_test