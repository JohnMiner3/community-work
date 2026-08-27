
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select `InvoiceLineId`
from `uc_sql_server_central`.`data_raw`.`invoiceline01`
where `InvoiceLineId` is null



  
  
      
    ) dbt_internal_test