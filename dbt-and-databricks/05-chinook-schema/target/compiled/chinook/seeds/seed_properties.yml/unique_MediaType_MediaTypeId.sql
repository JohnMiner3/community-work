
    
    

select
    MediaTypeId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."MediaType01"
where MediaTypeId is not null
group by MediaTypeId
having count(*) > 1


