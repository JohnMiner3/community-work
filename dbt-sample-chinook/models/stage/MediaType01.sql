
select *
from {{ source('Chinook', 'MediaType') }}