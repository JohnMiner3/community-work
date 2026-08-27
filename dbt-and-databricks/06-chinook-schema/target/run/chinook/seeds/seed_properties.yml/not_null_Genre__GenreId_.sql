
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `GenreId`
from `uc_sql_server_central`.`data_raw`.`genre01`
where `GenreId` is null



  
  
      
    ) dbt_internal_test