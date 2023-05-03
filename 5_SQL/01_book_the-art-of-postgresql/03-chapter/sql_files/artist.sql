-- Get the list of the N artists with the most albums
select 
	"Name" as name,
	count(*) albums 
from "Album" a 
join "Artist" a2 
using("ArtistId") 
group by name  
order by albums desc 
limit :n;

--psql -h 172.18.17.51 -U root -d general --variable "n=3" -f artist.sql