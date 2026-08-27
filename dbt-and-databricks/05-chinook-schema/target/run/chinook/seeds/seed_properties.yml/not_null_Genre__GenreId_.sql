
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "GenreId"
from "chinook01"."data_raw"."Genre01"
where "GenreId" is null



  
  
      
    ) dbt_internal_test