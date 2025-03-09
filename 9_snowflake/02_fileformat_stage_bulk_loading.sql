-- Create a database. A database automatically includes a schema named 'public'.

CREATE OR REPLACE DATABASE mydatabase;

-- Create target tables for CSV and JSON data. The tables are temporary, meaning they persist only for the duration of the user session and are not visible to other users.

CREATE OR REPLACE TEMPORARY TABLE mycsvtable (
  id INTEGER,
  last_name STRING,
  first_name STRING,
  company STRING,
  email STRING,
  workphone STRING,
  cellphone STRING,
  streetaddress STRING,
  city STRING,
  postalcode STRING);

CREATE OR REPLACE TEMPORARY TABLE myjsontable (
  json_data VARIANT);

-- Create a file format for CSV and JSON data.
CREATE OR REPLACE FILE FORMAT mycsvformat
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT myjsonformat
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- Create a stage for CSV and JSON data , stored in the Snowflake internal stage / snowflake managed.
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat;
CREATE OR REPLACE STAGE my_json_stage
  FILE_FORMAT = myjsonformat;

-- Execute the PUT command to upload the CSV & JSON files from your local file system.
PUT file:///tmp/load/contacts*.csv @my_csv_stage AUTO_COMPRESS=TRUE;
PUT file:///tmp/load/contacts.json @my_json_stage AUTO_COMPRESS=TRUE;

-- List the staged files.
LIST @my_csv_stage;
LIST @my_json_stage;

-- Copy the CSV and JSON data into the target tables.
COPY INTO mycsvtable
  FROM @my_csv_stage/contacts1.csv.gz
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  ON_ERROR = 'skip_file';

COPY INTO mycsvtable
  FROM @my_csv_stage
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  PATTERN='.*contacts[1-5].csv.gz'
  ON_ERROR = 'skip_file';

COPY INTO myjsontable
  FROM @my_json_stage/contacts.json.gz
  FILE_FORMAT = (FORMAT_NAME = myjsonformat)
  ON_ERROR = 'skip_file';

select * from mycsvtable;
select * from myjsontable;

-- resolve data load errors, JOB_ID comes from the snowflake query history
CREATE OR REPLACE TABLE save_copy_errors AS SELECT * FROM TABLE(VALIDATE(mycsvtable, JOB_ID=>'01bac2d5-3201-7936-0000-000c2c3e6459'));
SELECT * FROM TABLE(VALIDATE(mycsvtable, JOB_ID=>'01bac2d5-3201-7936-0000-000c2c3e6459'));

-- remove the succefully loaded files from the stage
REMOVE @my_csv_stage PATTERN='.*.csv.gz';
REMOVE @my_json_stage PATTERN='.*.json.gz';

-- cleanup
DROP DATABASE IF EXISTS mydatabase;