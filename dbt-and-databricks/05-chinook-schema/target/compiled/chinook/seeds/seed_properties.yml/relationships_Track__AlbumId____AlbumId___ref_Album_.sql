
    
    

with child as (
    select "AlbumId" as from_field
    from "chinook01"."data_raw"."Track01"
    where "AlbumId" is not null
),

parent as (
    select "AlbumId" as to_field
    from "chinook01"."data_raw"."Album01"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


