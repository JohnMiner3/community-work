select 
    [BookID]
      ,[Title]
      ,[Author]
      ,[ISBN]
      ,[Genre]
      ,[Quantity]
from {{ ref('books02') }}
where dbt_valid_to is NULL
