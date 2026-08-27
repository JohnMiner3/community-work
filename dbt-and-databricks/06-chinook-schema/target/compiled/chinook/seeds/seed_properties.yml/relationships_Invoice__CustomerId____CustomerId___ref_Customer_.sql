
    
    

with child as (
    select `CustomerId` as from_field
    from `uc_sql_server_central`.`data_raw`.`invoice01`
    where `CustomerId` is not null
),

parent as (
    select `CustomerId` as to_field
    from `uc_sql_server_central`.`data_raw`.`customer01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


