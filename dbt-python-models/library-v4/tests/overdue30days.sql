select * 
from {{ ref('loans01') }}
where ReturnDate is null
and {{ dbt.datediff('DueDate', dbt.current_timestamp(), 'day') }} >= 30 