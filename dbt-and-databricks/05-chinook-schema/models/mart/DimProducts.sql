select 
	{{ dbt_utils.generate_surrogate_key(['art."Name"', 'alb."Title"', 'trk."TrackId"']) }} AS ProductId,
	trk."TrackId" as "TrackId",
	art."Name" as "ArtistName",
	alb."Title" as "AlbumTitle",
	med."Name" as "MediaType",
	gen."Name" as "Genre",
	trk."Composer" as "Composer",
	trk."Milliseconds" as "Milliseconds",
	trk."Bytes" as "Bytes",
	trk."UnitPrice" as "UnitPrice"
from {{ ref('Track02') }} as trk
join {{ ref('Album02') }} as alb
on
	trk."AlbumId" = alb."AlbumId"
join {{ ref('Artist02') }} as art
on
	alb."ArtistId" = art."ArtistId"
join {{ ref('MediaType02') }} as med
on
	trk."MediaTypeId" = med."MediaTypeId"
join {{ ref('Genre02') }} as gen
on
	trk."GenreId" = gen."GenreId"

-- Apply effective date logic
where
	(trk."dbt_updated_at" >= alb."dbt_valid_from" and 
	trk."dbt_updated_at" <= {{ coalesce_to_date('alb.dbt_valid_to', "CAST('2100-01-01' AS DATE)") }} )

	and
	(trk."dbt_updated_at" >= art."dbt_valid_from" and 
	trk."dbt_updated_at" <= {{ coalesce_to_date('art.dbt_valid_to', "CAST('2100-01-01' AS DATE)") }} )

	and
	(trk."dbt_updated_at" >= med."dbt_valid_from" and 
	trk."dbt_updated_at" <= {{ coalesce_to_date('med.dbt_valid_to', "CAST('2100-01-01' AS DATE)") }} )

	and
	(trk."dbt_updated_at" >= gen."dbt_valid_from" and 
	trk."dbt_updated_at" <= {{ coalesce_to_date('gen.dbt_valid_to', "CAST('2100-01-01' AS DATE)") }} )