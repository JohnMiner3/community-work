
    
    

with child as (
    select "GenreId" as from_field
    from "chinook01"."data_raw"."Track01"
    where "GenreId" is not null
),

parent as (
    select "GenreId" as to_field
    from "chinook01"."data_raw"."Genre01"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


