
select *
from {{ source('Chinook', 'Genre') }}