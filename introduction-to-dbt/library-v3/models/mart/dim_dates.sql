select 
    [id]
      ,[date]
      ,[year]
      ,[quarter]
      ,[month]
      ,[month_name]
      ,[day]
      ,[day_of_week]
      ,[day_name]
      ,[week_of_year]
      ,[is_weekend]
from {{ ref('dates02') }}
where dbt_valid_to is NULL