
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "PlaylistId"
from "chinook01"."data_raw"."Playlist01"
where "PlaylistId" is null



  
  
      
    ) dbt_internal_test