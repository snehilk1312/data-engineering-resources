-- name: list-albums-by-artist
-- List the album titles and duration of a given artist
select 
	"Title" as album,
	sum("Milliseconds")*interval '1 ms' as duration 
from "Album" a  
join "Artist" a2 
using ("ArtistId") 
join "Track" t 
using ("AlbumId") 
where a2."Name" = :name
group by album
order by album;

--psql -h 172.18.17.51 -U root -d general --variable "name='Queen'" -f album.sql