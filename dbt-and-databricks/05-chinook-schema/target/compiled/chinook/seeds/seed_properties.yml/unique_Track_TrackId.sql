
    
    

select
    TrackId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Track01"
where TrackId is not null
group by TrackId
having count(*) > 1


