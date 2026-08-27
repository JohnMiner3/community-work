
    
    

with child as (
    select "MediaTypeId" as from_field
    from "chinook01"."data_raw"."Track01"
    where "MediaTypeId" is not null
),

parent as (
    select MediaTypeId as to_field
    from "chinook01"."data_raw"."MediaType01"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


