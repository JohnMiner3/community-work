
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    "CustomerId" as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Customer01"
where "CustomerId" is not null
group by "CustomerId"
having count(*) > 1



  
  
      
    ) dbt_internal_test