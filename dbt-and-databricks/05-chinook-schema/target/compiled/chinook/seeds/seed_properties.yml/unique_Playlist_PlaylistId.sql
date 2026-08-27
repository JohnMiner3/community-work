
    
    

select
    PlaylistId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Playlist01"
where PlaylistId is not null
group by PlaylistId
having count(*) > 1


