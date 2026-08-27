
    
    

select
    `AlbumId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`album01`
where `AlbumId` is not null
group by `AlbumId`
having count(*) > 1


