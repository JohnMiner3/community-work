
    
    

select
    `InvoiceLineId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`invoiceline01`
where `InvoiceLineId` is not null
group by `InvoiceLineId`
having count(*) > 1


