
    
    

select
    "InvoiceId" as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Invoice01"
where "InvoiceId" is not null
group by "InvoiceId"
having count(*) > 1


