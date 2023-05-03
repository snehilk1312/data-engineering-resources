-- Chapter 1 - SQL
-- Important command explained
-- create, alter, replace, using, substring, min, max, to_char
-- generate_series, extract, isodow, lag, interval, join, right join
-- left join, coalesce, locale, copy, cte, windowing, partition by, over


create table factbook
		(
			year int,
			date date,
			shares varchar,
			trades varchar,
			dollars varchar
		);

-- \copy factbook from '/home/snehil/snehi/Desktop/Notebook/data_science/SQL/01_book_the-art-of-postgresql/factbook.csv' with delimiter E' ' null '';
	
select * from factbook limit 5;

--2008	2008-05-02	1,720,434,642	9,486,091	$18,455,663,315
--2002	2002-06-04	1,161,832,049	78,247,652	$71,692,943,497
--2002	2002-07-08	4,153,856,062	73,637,273	$61,850,015,537
--2005	2005-08-04	4,625,953,905	39,777,603	$1,215,429,858
--2010	2010-05-12	9,584,795,000	35,961,933	$73,699,854,070

-- altering table 
-- commands used - alter, using, substring, replace
alter table factbook 
	alter shares
	 type bigint 
	 using replace(shares,',','')::bigint,
	alter trades
	 type bigint
	 using replace(trades,',','')::bigint,
	alter dollars
	 type bigint
	 using substring(replace(dollars,',','') from 2)::numeric;
	 
select * from factbook limit 5;
--2008	2008-05-02	1720434642	9486091	18455663315
--2002	2002-06-04	1161832049	78247652	71692943497
--2002	2002-07-08	4153856062	73637273	61850015537
--2005	2005-08-04	4625953905	39777603	1215429858
--2010	2010-05-12	9584795000	35961933	73699854070

-- min,max
select min(shares),max(shares),min(trades),max(trades),min(dollars),max(dollars) from factbook;

-- interval
-- to_char(expression,format)
select date,to_char(date,'Dy') as day,
	   to_char(shares,'9G999G999G999') as shares,
	   to_char(trades,'99G999G999') as trades,
	   to_char(dollars,'L99G999G999G999') as dollars
from factbook 
where date>='2017-02-01'::date and date < '2017-02-01'::date + interval '1 month' order by date;
--2017-02-01	Wed	   259,212,278	 85,634,252	$ 21,672,018,818
--2017-02-02	Thu	 4,202,181,400	 40,511,380	$ 90,687,745,439
--2017-02-04	Sat	 1,097,443,470	 86,018,627	$ 49,327,741,981
--2017-02-05	Sun	 2,919,966,484	 95,612,048	$ 10,427,960,865
--2017-02-06	Mon	 5,963,137,012	 58,786,188	$ 30,997,644,229

-- sql generate series 
select generate_series(1,100);
--1
--2
--3
--4
--5
select generate_series(0,100,5);
--0
--5
--10
--15
--20
-- select the contigiuous date between two given dates
select generate_series('2017-02-01'::date, ('2017-02-01'::date + interval '1 month' - interval '1 day')::date, interval '1 day')::date; 
--2017-02-01
--2017-02-02
--2017-02-03
--2017-02-04
--2017-02-05

select calendar.entry::date as date,shares,trades,dollars  from 
generate_series('2017-02-01'::date, ('2017-02-01'::date + interval '1 month' - interval '1 day')::date, interval '1 day') as calendar(entry) 
left join factbook on factbook.date=calendar.entry order by date;
-- OR
select entry::date as date,coalesce (shares,0),coalesce (trades,0),to_char(coalesce(dollars,0),'L99G999G999G999') from factbook right join 
generate_series('2017-02-01'::date, ('2017-02-01'::date+interval '1 month'-interval '1 day')::date, interval '1 day') as calendar(entry) 
on factbook.date=calendar.entry order by entry;
--2017-02-01	259212278	85634252	21672018818
--2017-02-02	4202181400	40511380	90687745439
--2017-02-03			
--2017-02-04	1097443470	86018627	49327741981
--2017-02-05	2919966484	95612048	10427960865

select entry::date,extract('isodow' from entry) from generate_series(('2017-02-01'::date - interval '1 week')::date,('2017-02-01'::date + interval '1 month'-interval '1 day')::date,interval '1 day') calendar(entry);
--2017-01-25
--2017-01-26
--2017-01-27
--2017-01-28
--2017-01-29
--2017-01-30
--2017-01-31
--2017-02-01
--2017-02-02
--2017-02-03
--2017-02-04
--2017-02-05

select 
	entry::date,
	extract('isodow' from entry) 
    from generate_series(('2017-02-01'::date - interval '1 week')::date,('2017-02-01'::date + interval '1 month'-interval '1 day')::date,interval '1 day') calendar(entry)
    order by date_part,entry;
--2017-01-30	1.0
--2017-02-06	1.0
--2017-02-13	1.0
--2017-02-20	1.0
--2017-02-27	1.0
--2017-01-31	2.0
--2017-02-07	2.0
--2017-02-14	2.0
--2017-02-21	2.0
--2017-02-28	2.0


-- calculating WoW growth
with computed_data as(
	select entry::date as date,
		   coalesce (shares,0) shares,
		   coalesce (trades,0) trades,
		   coalesce(dollars,0) dollars,
	       lag(dollars,1) over(partition by extract('isodow' from entry)  order by date) as last_week_dollar
	from factbook right join
	generate_series(('2017-02-01'::date - interval '1 week')::date,('2017-02-01'::date + interval '1 month'-interval '1 day')::date,interval '1 day') 
	as calendar(entry)
	on factbook.date=calendar.entry order by entry
),
final as(
select date,to_char(date, 'Dy') as day ,shares,trades,to_char(dollars,'L99G999G999G999') dollars,last_week_dollar,case when dollars is not null and dollars<>0
then round(100.0*(dollars-last_week_dollar)/dollars,2) end as  growth 
from computed_data where date>='2017-02-01'
)
select * from final;

-- chatgpt explanation 
--This SQL code is using common table expressions (CTEs) to generate a report. 
--The first CTE, computed_data, generates a series of dates between 2017-01-25 and 2017-02-28 using the generate_series function.
--It then performs a right join with the factbook table on the date column. 
--The result of this join includes all the dates in the generated series and any matching data from the factbook table. 
--The coalesce function is used to replace any null values in the shares, trades, and dollars columns with 0. 
--The lag function is used to get the value of dollars from the previous week.
--The second CTE, final, selects data from the computed_data CTE where the date is greater than 2017-02-01. 
--It also calculates a growth column by taking the percentage change between dollars and last_week_dollar.
--Finally, the main query selects all data from the final CTE. 
--This code generates a report that shows shares, trades, dollars, last_week_dollar, and growth for each date between 2017-02-01 and 2017-03-01.


--2017-02-01	Wed	259212278	85634252	$ 21,672,018,818	29037356631	-33.99
--2017-02-02	Thu	4202181400	40511380	$ 90,687,745,439		
--2017-02-03	Fri	0	0	$              0		
--2017-02-04	Sat	1097443470	86018627	$ 49,327,741,981	16575203343	66.40
--2017-02-05	Sun	2919966484	95612048	$ 10,427,960,865		
--2017-02-06	Mon	5963137012	58786188	$ 30,997,644,229	18301662052	40.96
--2017-02-07	Tue	3008491803	51489666	$ 63,384,939,853		
--2017-02-08	Wed	0	0	$              0		
--2017-02-09	Thu	2675528380	52881019	$ 68,259,643,193	90687745439	-32.86
--2017-02-10	Fri	3130404454	85695024	$ 65,137,270,238	86797254182	-33.25