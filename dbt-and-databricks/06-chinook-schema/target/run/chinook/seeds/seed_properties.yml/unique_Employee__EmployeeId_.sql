
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    `EmployeeId` as unique_field,
    count(*) as n_records

from `uc_sql_server_central`.`data_raw`.`employee01`
where `EmployeeId` is not null
group by `EmployeeId`
having count(*) > 1



  
  
      
    ) dbt_internal_test