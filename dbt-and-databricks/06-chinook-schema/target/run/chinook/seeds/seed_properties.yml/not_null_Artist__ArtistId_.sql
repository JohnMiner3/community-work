
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `ArtistId`
from `uc_sql_server_central`.`data_raw`.`artist01`
where `ArtistId` is null



  
  
      
    ) dbt_internal_test