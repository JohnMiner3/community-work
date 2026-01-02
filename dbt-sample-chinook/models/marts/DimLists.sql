select 
	pl.Name as ListName,
	pl.PlaylistId as ListId,
	plt.TrackId
from 
	{{ ref('Playlist02') }} as pl
join 
	{{ ref('PlaylistTrack02') }} as plt
on 
	pl.PlaylistId = plt.PlaylistId
