-- Chapter 2 - SQL
-- Important command explained
-- pg_size_pretty, time-interval, stored procedures, lateral joins

select 
	t."Name" as track,
	g."Name" as genre  
from "Track" t 
    join "Genre" g ON t."GenreId" = g."GenreId"  
	where "AlbumId"=193 
	order by "TrackId" ;

-- showing the duration of track and its size

-- making number/sizes readable
select pg_size_pretty(1048::bigint);
--1048 bytes
select pg_size_pretty(16384::bigint);
--16 kB
select pg_size_pretty(16384::numeric);
--16 kB

-- unix seconds passed -> gives in hour:min:sec
select 1683118192*interval '1 s'
--467532:49:52

select 1683118192*interval '1000 ms'
--467532:49:52

select "Name" ,"Milliseconds" * interval '0.001 s' as duration,pg_size_pretty(t."Bytes"::bigint)  from "Track" t 
--For Those About To Rock (We Salute You)	00:05:43.719	11 MB
--Balls to the Wall	00:05:42.562	5381 kB
--Fast As a Shark	00:03:50.619	3897 kB
--Restless and Wild	00:04:12.051	4230 kB
--Princess of the Dawn	00:06:15.418	6143 kB


-- display the list of albums from a given artist, each with its total duration
select 
	a."Title" ,
	sum(t."Milliseconds")*interval '1 ms' as duration 
from "Album" a 
join 
	"Artist" a2 on a."ArtistId" =a2."ArtistId" 
join 
	"Track" t on t."AlbumId" =a."AlbumId" 
where 
	a2."Name" ='Led Zeppelin'
group by 
	a."Title" 
order by 
	a."Title" ;
--BBC Sessions [Disc 1] [Live]	01:14:49.92
--BBC Sessions [Disc 2] [Live]	01:18:19.108
--Coda	00:33:01.123
--Houses Of The Holy	00:40:54.435
--In Through The Out Door	00:42:35.478
--IV	00:42:37.462
--Led Zeppelin I	00:44:50.399


-- creating stored procedures
create or replace function get_all_albums(
	in name text,
	out album text,
	out duration interval 
) 
returns setof record
language sql 
as $$
select 
	a."Title" as album,
	sum(t."Milliseconds")*interval '1 ms' as duration 
from "Album" a 
join 
	"Artist" a2 on a."ArtistId" =a2."ArtistId" 
join 
	"Track" t on t."AlbumId" =a."AlbumId" 
where 
	a2."Name" =get_all_albums.name
group by 
	a."Title" 
order by 
	a."Title" ;
$$;

select * from get_all_albums('Pink Floyd')
--Dark Side Of The Moon	00:42:52.638

select * from get_all_albums('The Doors')
--The Doors	00:44:29.734

select * from get_all_albums('Nirvana')
--From The Muddy Banks Of The Wishkah [Live]	00:54:03.381
--Nevermind	00:42:37.066

--list the album with durations of the artists who have exactly four albums registered in our database
with four_albums as (
select a."ArtistId" as artistid,a2."Name" as name,count(*) as  num_album  from "Album" a join "Artist" a2 on a."ArtistId" =a2."ArtistId"  group by a."ArtistId",a2."Name"  having count(*) = 4),
final as(
	select four_albums.name as artist_name,album,duration from four_albums ,lateral get_all_albums(name)
)
select * from final;
--Faith No More	Album Of The Year	00:43:06.64
--Faith No More	Angel Dust	01:01:49.719
--Faith No More	King For A Day Fool For A Lifetime	00:59:49.791
--Faith No More	The Real Thing	00:55:25.748
--Van Halen	Diver Down	00:31:30.424
--Van Halen	The Best Of Van Halen, Vol. I	01:12:29.98
--Van Halen	Van Halen	00:35:33.413
--Van Halen	Van Halen III	01:05:23.402
--Various Artists	Ax� Bahia 2001	00:47:02.944
--Various Artists	Carnaval 2001	01:05:26.015
--Various Artists	Sambas De Enredo 2001	01:13:08.174
--Various Artists	Vozes do MPB	00:47:38.678
--Lost	Lost, Season 1	18:00:54.936
--Lost	Lost, Season 2	17:34:49.631
--Lost	Lost, Season 3	19:37:45.582
--Lost	LOST, Season 4	10:57:48.433
--Foo Fighters	In Your Honor [Disc 1]	00:40:09.295
--Foo Fighters	In Your Honor [Disc 2]	00:43:16.358
--Foo Fighters	One By One	00:55:03.622
--Foo Fighters	The Colour And The Shape	00:46:55.29
