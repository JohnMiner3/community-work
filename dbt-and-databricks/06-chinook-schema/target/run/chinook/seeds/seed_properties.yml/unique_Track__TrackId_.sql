
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `TrackId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`track01`
where `TrackId` is not null
group by `TrackId`
having count(*) > 1



  
  
      
    ) dbt_internal_test