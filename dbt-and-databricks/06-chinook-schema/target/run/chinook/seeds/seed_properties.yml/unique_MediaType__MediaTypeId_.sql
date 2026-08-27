
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `MediaTypeId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`mediatype01`
where `MediaTypeId` is not null
group by `MediaTypeId`
having count(*) > 1



  
  
      
    ) dbt_internal_test