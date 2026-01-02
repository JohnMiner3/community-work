select *
from {{ source('Chinook', 'Artist') }}