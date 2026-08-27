
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `GenreId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`genre01`
where `GenreId` is not null
group by `GenreId`
having count(*) > 1



  
  
      
    ) dbt_internal_test