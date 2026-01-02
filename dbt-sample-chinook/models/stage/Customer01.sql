select *
from {{ source('Chinook', 'Customer') }}