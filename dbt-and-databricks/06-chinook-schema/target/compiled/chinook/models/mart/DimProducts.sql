select 
	md5(cast(concat(coalesce(cast(art.`Name` as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(alb.`Title` as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(trk.`TrackId` as string), '_dbt_utils_surrogate_key_null_')) as string)) AS ProductId,
	trk.`TrackId` as `TrackId`,
	art.`Name` as `ArtistName`,
	alb.`Title` as `AlbumTitle`,
	med.`Name` as `MediaType`,
	gen.`Name` as `Genre`,
	trk.`Composer` as `Composer`,
	trk.`Milliseconds` as `Milliseconds`,
	trk.`Bytes` as `Bytes`,
	trk.`UnitPrice` as `UnitPrice`
from `uc_sql_server_central`.`data_snapshot`.`track02` as trk
join `uc_sql_server_central`.`data_snapshot`.`album02` as alb
on
	trk.`AlbumId` = alb.`AlbumId`
join `uc_sql_server_central`.`data_snapshot`.`artist02` as art
on
	alb.`ArtistId` = art.`ArtistId`
join `uc_sql_server_central`.`data_snapshot`.`mediatype02` as med
on
	trk.`MediaTypeId` = med.`MediaTypeId`
join `uc_sql_server_central`.`data_snapshot`.`genre02` as gen
on
	trk.`GenreId` = gen.`GenreId`

-- Apply effective date logic
where
	(trk.`dbt_updated_at` >= alb.`dbt_valid_from` and 
	trk.`dbt_updated_at` <= 
  COALESCE(
    alb.dbt_valid_to,
    CAST('2100-01-01' AS DATE)
  )
 )

	and
	(trk.`dbt_updated_at` >= art.`dbt_valid_from` and 
	trk.`dbt_updated_at` <= 
  COALESCE(
    art.dbt_valid_to,
    CAST('2100-01-01' AS DATE)
  )
 )

	and
	(trk.`dbt_updated_at` >= med.`dbt_valid_from` and 
	trk.`dbt_updated_at` <= 
  COALESCE(
    med.dbt_valid_to,
    CAST('2100-01-01' AS DATE)
  )
 )

	and
	(trk.`dbt_updated_at` >= gen.`dbt_valid_from` and 
	trk.`dbt_updated_at` <= 
  COALESCE(
    gen.dbt_valid_to,
    CAST('2100-01-01' AS DATE)
  )
 )