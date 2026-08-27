
    
    

with child as (
    select `AlbumId` as from_field
    from `uc_sql_server_central`.`data_raw`.`track01`
    where `AlbumId` is not null
),

parent as (
    select `AlbumId` as to_field
    from `uc_sql_server_central`.`data_raw`.`album01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


