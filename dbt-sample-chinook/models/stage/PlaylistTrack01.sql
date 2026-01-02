select *
from {{ source('Chinook', 'PlaylistTrack') }}