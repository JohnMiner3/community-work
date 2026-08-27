
      
  
    
        create or replace table `uc_sql_server_central`.`data_snapshot`.`playlisttrack02`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select *,
        md5(coalesce(cast(`skey` as string ), '')
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
        select 
  `PlaylistId`,
  `TrackId`,
  `skey`
from 
(
  select 
    `PlaylistId`,
    `TrackId`,
    md5(cast(concat(coalesce(cast(`PlaylistId` as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(`TrackId` as string), '_dbt_utils_surrogate_key_null_')) as string)) as `skey`
    from `uc_sql_server_central`.`data_raw`.`playlisttrack01`
) as d
    ) sbq


  
  