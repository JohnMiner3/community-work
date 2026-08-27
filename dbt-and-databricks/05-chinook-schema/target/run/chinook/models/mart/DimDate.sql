
  
    

  create  table "chinook01"."data_mart"."DimDate__dbt_tmp"
  
  
    as
  
  (
    select *
from "chinook01"."data_stage"."Dates02"
  );
  