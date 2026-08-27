
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `CustomerId`
from `uc_sql_server_central`.`data_raw`.`customer01`
where `CustomerId` is null



  
  
      
    ) dbt_internal_test