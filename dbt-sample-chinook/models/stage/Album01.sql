select *
from {{ source('Chinook', 'Album') }}