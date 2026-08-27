
    
    

with child as (
    select `GenreId` as from_field
    from `uc_sql_server_central`.`data_raw`.`track01`
    where `GenreId` is not null
),

parent as (
    select `GenreId` as to_field
    from `uc_sql_server_central`.`data_raw`.`genre01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


