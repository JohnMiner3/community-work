select 
	pl.`Name` as `ListName`,
	pl.`PlaylistId` as `ListId`,
	plt.`TrackId` as `TrackId`
from 
	`uc_sql_server_central`.`data_snapshot`.`playlist02` as pl
join 
	`uc_sql_server_central`.`data_snapshot`.`playlisttrack02` as plt
on 
	pl.`PlaylistId` = plt.`PlaylistId`