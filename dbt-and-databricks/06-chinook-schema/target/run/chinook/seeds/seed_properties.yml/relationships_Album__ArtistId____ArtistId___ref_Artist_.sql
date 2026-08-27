
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select `ArtistId` as from_field
    from `uc_sql_server_central`.`data_raw`.`album01`
    where `ArtistId` is not null
),

parent as (
    select `ArtistId` as to_field
    from `uc_sql_server_central`.`data_raw`.`artist01`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test