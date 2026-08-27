
    
    

select
    `EmployeeId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`employee01`
where `EmployeeId` is not null
group by `EmployeeId`
having count(*) > 1


