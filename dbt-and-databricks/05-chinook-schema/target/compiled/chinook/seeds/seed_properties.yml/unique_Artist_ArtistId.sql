
    
    

select
    ArtistId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Artist01"
where ArtistId is not null
group by ArtistId
having count(*) > 1


