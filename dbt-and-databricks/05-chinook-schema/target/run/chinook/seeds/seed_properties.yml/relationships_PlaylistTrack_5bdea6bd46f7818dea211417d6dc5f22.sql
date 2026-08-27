
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select "PlaylistId" as from_field
    from "chinook01"."data_raw"."PlaylistTrack01"
    where "PlaylistId" is not null
),

parent as (
    select "PlaylistId" as to_field
    from "chinook01"."data_raw"."Playlist01"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test