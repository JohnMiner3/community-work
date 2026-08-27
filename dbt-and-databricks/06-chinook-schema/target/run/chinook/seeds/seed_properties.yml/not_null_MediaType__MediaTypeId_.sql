
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `MediaTypeId`
from `uc_sql_server_central`.`data_raw`.`mediatype01`
where `MediaTypeId` is null



  
  
      
    ) dbt_internal_test