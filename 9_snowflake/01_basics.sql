-- create a snowflake db
create database test_db;

-- see current database, schema
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- create a basic table
CREATE OR REPLACE TABLE emp_basic (
   first_name STRING ,
   last_name STRING ,
   email STRING ,
   streetaddress STRING ,
   city STRING ,
   start_date DATE
   );

-- Staging sample data files
PUT file:///path_to_data/employees0*.csv @test_db.public.%emp_basic;

-- Listing the staged files
LIST @test_db.public.%emp_basic;

-- Copy the data 
COPY INTO emp_basic
  FROM @%emp_basic
  FILE_FORMAT = (type = csv field_optionally_enclosed_by='"')
  PATTERN = '.*employees0[1-5].csv.gz'
  ON_ERROR = 'skip_file';

-- Use, insert query instead to manually insert
-- Insert
INSERT INTO emp_basic VALUES
   ('Clementine','Adamou','cadamou@sf_tuts.com','10510 Sachs Road','Klenak','2017-9-22') ,
   ('Marlowe','De Anesy','madamouc@sf_tuts.co.uk','36768 Northfield Plaza','Fangshan','2017-1-26');