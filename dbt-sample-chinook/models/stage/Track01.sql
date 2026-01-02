
select 
    TrackId,
    Name,
    AlbumId,
    MediaTypeId,
    GenreId,
    {{ replace_null_with_empty('Composer') }} as Composer,
    Milliseconds,
    Bytes,
    UnitPrice
from {{ source('Chinook', 'Track') }}