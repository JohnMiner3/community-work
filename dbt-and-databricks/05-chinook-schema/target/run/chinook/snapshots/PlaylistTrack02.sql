
      
  
    

  create  table "chinook01"."data_snapshot"."PlaylistTrack02"
  
  
    as
  
  (
    
    

    select *,
        md5(coalesce(cast("skey" as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        
  
  coalesce(nullif(now()::timestamp without time zone, now()::timestamp without time zone), null)
  as dbt_valid_to
from (
        



select 
  "PlaylistId",
  "TrackId",
  "skey"
from 
(
  select 
    "PlaylistId",
    "TrackId",
    md5(cast(coalesce(cast("PlaylistId" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("TrackId" as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as "skey"
    from "chinook01"."data_raw"."PlaylistTrack01"
) as d

    ) sbq



  );
  
  