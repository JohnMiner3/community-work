
    
    

with child as (
    select `TrackId` as from_field
    from `uc_sql_server_central`.`data_raw`.`playlisttrack01`
    where `TrackId` is not null
),

parent as (
    select `TrackId` as to_field
    from `uc_sql_server_central`.`data_raw`.`track01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


