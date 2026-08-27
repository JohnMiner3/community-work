
  
    
        create or replace table `uc_sql_server_central`.`data_stage`.`invoiceline02`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select
    *
from `uc_sql_server_central`.`data_raw`.`invoiceline01`
  