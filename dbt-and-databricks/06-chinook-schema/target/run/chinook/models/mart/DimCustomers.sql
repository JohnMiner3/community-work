
  
    
        create or replace table `uc_sql_server_central`.`data_mart`.`dimcustomers`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select *
from `uc_sql_server_central`.`data_stage`.`customer02`
  