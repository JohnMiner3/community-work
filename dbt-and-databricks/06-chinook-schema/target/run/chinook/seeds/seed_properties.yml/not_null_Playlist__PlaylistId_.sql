
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `PlaylistId`
from `uc_sql_server_central`.`data_raw`.`playlist01`
where `PlaylistId` is null



  
  
      
    ) dbt_internal_test