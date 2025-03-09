-- The CREATE DATABASE statement creates a database. The database automatically includes a schema named ‘public’.
CREATE OR REPLACE DATABASE mydatabase;

USE SCHEMA mydatabase.public;

-- The CREATE TABLE statement creates a target table for JSON data.
CREATE OR REPLACE TABLE raw_source (
  SRC VARIANT);

-- The CREATE STAGE statement creates an external stage that points to the S3 bucket containing the sample file for this tutorial.
CREATE OR REPLACE STAGE my_stage
  URL = 's3://snowflake-docs/tutorials/json';

LIST @my_stage;

-- copy the JSON data into the target table.
COPY INTO raw_source
  FROM @my_stage/server/2.6/2016/07/15/15
  FILE_FORMAT = (TYPE = JSON);

SELECT * FROM raw_source;
select src:device_type from raw_source;
select src:events[0]:v from raw_source;
