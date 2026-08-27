
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    AlbumId as unique_field,
    count(*) as n_records

from "chinook01"."data_raw"."Album01"
where AlbumId is not null
group by AlbumId
having count(*) > 1



  
  
      
    ) dbt_internal_test