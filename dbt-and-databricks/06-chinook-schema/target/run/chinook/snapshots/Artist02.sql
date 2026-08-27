
      
  
    
        create or replace table `uc_sql_server_central`.`data_snapshot`.`artist02`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select *,
        md5(coalesce(cast(`ArtistId` as string ), '')
         || '|' || coalesce(cast(
    current_timestamp()
 as string ), '')
        ) as dbt_scd_id,
        
    current_timestamp()
 as dbt_updated_at,
        
    current_timestamp()
 as dbt_valid_from,
        
  
  coalesce(nullif(
    current_timestamp()
, 
    current_timestamp()
), null)
  as dbt_valid_to

    from (
        select * from `uc_sql_server_central`.`data_raw`.`artist01`
    ) sbq


  
  