
  
    
        create or replace table `uc_sql_server_central`.`data_mart`.`dimemployees`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select *
from `uc_sql_server_central`.`data_snapshot`.`employee02`
  