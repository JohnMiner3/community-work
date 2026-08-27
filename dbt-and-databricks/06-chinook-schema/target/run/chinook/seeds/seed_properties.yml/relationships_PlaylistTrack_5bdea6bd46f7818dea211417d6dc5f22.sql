
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select `PlaylistId` as from_field
    from `uc_sql_server_central`.`data_raw`.`playlisttrack01`
    where `PlaylistId` is not null
),

parent as (
    select `PlaylistId` as to_field
    from `uc_sql_server_central`.`data_raw`.`playlist01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test