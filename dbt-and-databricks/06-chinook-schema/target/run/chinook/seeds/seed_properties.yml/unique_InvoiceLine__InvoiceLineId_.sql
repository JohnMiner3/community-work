
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `InvoiceLineId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`invoiceline01`
where `InvoiceLineId` is not null
group by `InvoiceLineId`
having count(*) > 1



  
  
      
    ) dbt_internal_test