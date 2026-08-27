
    
    

select
    "CustomerId" as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Customer01"
where "CustomerId" is not null
group by "CustomerId"
having count(*) > 1


