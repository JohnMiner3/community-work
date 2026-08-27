
    
    

select
    `TrackId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`track01`
where `TrackId` is not null
group by `TrackId`
having count(*) > 1


