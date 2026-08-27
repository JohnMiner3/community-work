
    
    

select
    AlbumId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Album01"
where AlbumId is not null
group by AlbumId
having count(*) > 1


