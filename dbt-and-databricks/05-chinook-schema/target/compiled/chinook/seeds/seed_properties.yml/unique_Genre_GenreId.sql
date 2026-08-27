
    
    

select
    GenreId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Genre01"
where GenreId is not null
group by GenreId
having count(*) > 1


