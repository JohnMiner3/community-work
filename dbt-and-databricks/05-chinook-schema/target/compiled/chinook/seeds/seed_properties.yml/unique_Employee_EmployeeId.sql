
    
    

select
    EmployeeId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Employee01"
where EmployeeId is not null
group by EmployeeId
having count(*) > 1


