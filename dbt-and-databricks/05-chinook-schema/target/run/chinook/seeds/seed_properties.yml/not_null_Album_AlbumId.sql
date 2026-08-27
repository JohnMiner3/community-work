
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select AlbumId
from "chinook01"."data_raw"."Album01"
where AlbumId is null



  
  
      
    ) dbt_internal_test