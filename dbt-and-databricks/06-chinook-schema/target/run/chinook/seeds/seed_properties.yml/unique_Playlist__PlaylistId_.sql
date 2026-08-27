
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `PlaylistId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`playlist01`
where `PlaylistId` is not null
group by `PlaylistId`
having count(*) > 1



  
  
      
    ) dbt_internal_test