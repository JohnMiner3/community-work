
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `InvoiceId`
from `uc_sql_server_central`.`data_raw`.`invoice01`
where `InvoiceId` is null



  
  
      
    ) dbt_internal_test