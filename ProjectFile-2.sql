
select get_ddl('table','tblnyc_parking_source');

--Source table 
create or replace TRANSIENT TABLE TBLNYC_PARKING_SOURCE (
	FILENAME VARCHAR(16777216),
	ROWID NUMBER(18,0),
	SUMMONSNUMBER VARCHAR(16777216),
	PLATEID VARCHAR(16777216),
	REGISTRATIONSTATE VARCHAR(16777216),
	PLATETYPE VARCHAR(16777216),
	ISSUEDATE VARCHAR(16777216),
	VIOLATIONCODE VARCHAR(16777216),
	VEHICLEBODYTYPE VARCHAR(16777216),
	VEHICLEMAKE VARCHAR(16777216),
	ISSUINGAGENCY VARCHAR(16777216)
);

--Snowpipe 
create or replace pipe snowpipe_nyc_data
auto_ingest=true
error_integration = PIPE_ERROR_SNS
as
copy into tblnyc_parking_source 
from (
select 
metadata$filename as FileName,
metadata$file_row_number as RowId,
replace(nyc.$1,'"','') as SummonsNumber ,
nyc.$2 as PlateID,
nyc.$3 as RegistrationState,
nyc.$4 as PlateType,
nyc.$5 as IssueDate,
nyc.$6 as ViolationCode,
nyc.$7 as VehicleBodyType,
nyc.$8 as VehicleMake,
nyc.$9 as IssuingAgency
from @STG_NYC_EXTERNAL
(file_format => 'ff_csv') nyc 
);

--Check status of snowpipe
select PARSE_JSON(system$pipe_status('snowpipe_nyc_data'));

--To load the historical data from stage
alter pipe snowpipe_nyc_data refresh;

--Validate the pipe load status
select * from 
table(VALIDATE_PIPE_LOAD(pipe_name => 'snowpipe_nyc_data',start_time =>  DATEADD('MINUTE',-5,CURRENT_TIMESTAMP())  ));

--Check the errors in the file using copy_history table function
select * from 
table(information_schema.copy_history(table_name => 'tblnyc_parking_source', start_time => DATEADD('MINUTE',-5,CURRENT_TIMESTAMP())));


--stream object for NJ data
create or replace stream stream_nyc_parking_nj
on table tblnyc_parking_source;

--stream object for NY data
create or replace stream stream_nyc_parking_ny
on table tblnyc_parking_source;


show streams;

--Validate the stream data 
select * from stream_nyc_parking_nj limit 10;
select * from stream_nyc_parking_ny limit 10;

desc stream stream_nyc_parking_nj;



