
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select `InvoiceId` as from_field
    from `uc_sql_server_central`.`data_raw`.`invoiceline01`
    where `InvoiceId` is not null
),

parent as (
    select `InvoiceId` as to_field
    from `uc_sql_server_central`.`data_raw`.`invoice01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test