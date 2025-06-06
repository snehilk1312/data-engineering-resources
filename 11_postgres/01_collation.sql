create table test1 (
	a text collate "de_DE",
	b text collate "es_ES"
);

-- German collation examples
INSERT INTO test1 (a, b) VALUES
('äpfel', 'árbol'),     -- 'ä' in German is sorted after 'a'
('apfel', 'azul'),      -- normal 'a' word
('zebra', 'zanahoria'), -- to test end-of-alphabet sorting
('straße', 'sí'),       -- 'ß' in German has special rules

-- Spanish collation examples
('alpha', 'llama'),     -- 'll' is a distinct letter in traditional Spanish
('beta', 'luz'),
('gamma', 'chico'),     -- 'ch' is also treated specially
('delta', 'niño');      -- 'ñ' is distinct from 'n' in Spanish


select * from test1;

SELECT * FROM test1 ORDER BY a;

SELECT * FROM test1 ORDER BY b;


-- the < comparison is performed according to de_DE rules, because the expression combines 
-- an implicitly derived collation with the default collation. 
SELECT a,a < 'foo' FROM test1;

-- the comparison is performed using fr_FR rules, 
--because the explicit collation derivation overrides the implicit one.
SELECT a,a < ('foo' COLLATE "fr_FR") FROM test1;

-- the parser cannot determine which collation to apply, since the a and b columns have
-- conflicting implicit collations
SELECT a < b FROM test1; -- error, The error can be resolved by attaching an explicit collation specifier to either input expression

-- avoid above error
SELECT a < b COLLATE "de_DE" FROM test1;

-- doesn't result in an error, because || operator doesn't care about collations: its result
-- is the same regardless of collation
SELECT a || b FROM test1;


-- more example, in case of a function or operator's combined input expressions
SELECT * FROM test1 ORDER BY a || 'foo'; -- ordering will be done according to de_DE rules.
SELECT * FROM test1 ORDER BY a || b; -- error, results in an error, because even though the || operator doesn't need to know a collation, the ORDER BY clause does
SELECT * FROM test1 ORDER BY a || b COLLATE "fr_FR"; -- use explicit collation to avoid above error



-- Predefined collations
SELECT * FROM pg_collation;

-- PostgreSQL considers distinct collation objects to be incompatible even when they have identical properties. Thus for example,
SELECT a COLLATE "C" < b COLLATE "POSIX" FROM test1; -- will draw an error even though the C and POSIX collations have identical behaviours

-- Check default collation with
SHOW LC_COLLATE;
SHOW LC_CTYPE;


-- Native support for case-insensitive collation has been added in PostgreSQL v12
-- we can implement using

CREATE COLLATION case_insensitive (
  provider = icu,
  locale = 'und-u-ks-level2',
  deterministic = false
);

CREATE TABLE names(
  first_name text,
  last_name text
);

insert into names values
  ('Anton','Egger'),
  ('Berta','egger'),
  ('Conrad','Egger');

select * from names
  order by
    last_name collate case_insensitive,
    first_name collate case_insensitive;

-- output showing case insensitive collation
--Anton	Egger
--Berta	egger
--Conrad	Egger

select 'hello' = 'HELLO'; -- false
select 'hello' = 'HELLO' COLLATE case_insensitive; 