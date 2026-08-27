
    
    

select
    `ArtistId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`artist01`
where `ArtistId` is not null
group by `ArtistId`
having count(*) > 1


