
  
    
        create or replace table `uc_sql_server_central`.`data_mart`.`dimdate`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select *
from `uc_sql_server_central`.`data_stage`.`dates02`
  