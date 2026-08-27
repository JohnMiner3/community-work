
    
    

select
    InvoiceLineId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."InvoiceLine01"
where InvoiceLineId is not null
group by InvoiceLineId
having count(*) > 1


