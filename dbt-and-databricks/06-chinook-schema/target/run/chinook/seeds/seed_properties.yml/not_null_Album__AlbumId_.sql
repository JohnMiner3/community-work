
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `AlbumId`
from `uc_sql_server_central`.`data_raw`.`album01`
where `AlbumId` is null



  
  
      
    ) dbt_internal_test