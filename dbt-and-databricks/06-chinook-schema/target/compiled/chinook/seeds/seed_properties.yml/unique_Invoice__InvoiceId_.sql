
    
    

select
    `InvoiceId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`invoice01`
where `InvoiceId` is not null
group by `InvoiceId`
having count(*) > 1


