
  
    
        create or replace table `uc_sql_server_central`.`data_stage`.`invoice02`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select
    *
from `uc_sql_server_central`.`data_raw`.`invoice01`
  