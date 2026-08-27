
    
    

select
    `CustomerId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`customer01`
where `CustomerId` is not null
group by `CustomerId`
having count(*) > 1


