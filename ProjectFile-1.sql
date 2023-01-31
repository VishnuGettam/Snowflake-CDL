--SNOWFLAKE_AWS_INTEGRATION
--PIPE_ERROR_SNS

--DB creation
create or replace database assignment_db;

--Source Schema
create or replace schema source;


desc integration SNOWFLAKE_AWS_INTEGRATION;

--CSV file format 
create or replace file format ff_csv
type = 'CSV'
field_delimiter=','
record_delimiter='\n' 
encoding = 'utf-8' 
skip_header = 1;

desc file format ff_csv;

--external stage creation
create or replace stage stg_nyc_external
url = 's3://snowflake-dev-s3/Load/CSV/'
storage_integration = SNOWFLAKE_AWS_INTEGRATION
file_format =(format_name = 'ff_csv');

show stages;

list @stg_nyc_external;















