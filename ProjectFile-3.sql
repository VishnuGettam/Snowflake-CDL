
--Target table for NY
create or replace TRANSIENT TABLE TBLPARKINGNY (
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

--Target table for NJ
create or replace TRANSIENT TABLE TBLPARKINGNJ (
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

--task to check the stream data and load into target table 
create or replace task tsk_parking_ny
warehouse = 'WAREHOUSE_M'
schedule = '1 minute'
when system$stream_has_data('stream_nyc_parking_ny')
as
merge into TBLPARKINGNY 
using stream_nyc_parking_ny
on stream_nyc_parking_ny.SUMMONSNUMBER = TBLPARKINGNY.SUMMONSNUMBER 
when matched and metadata$action = 'INSERT' and metadata$isupdate = 'TRUE' and stream_nyc_parking_ny.REGISTRATIONSTATE = 'NY' then
update set 
SUMMONSNUMBER = stream_nyc_parking_ny.SUMMONSNUMBER   ,
PLATEID = stream_nyc_parking_ny.PLATEID  ,
REGISTRATIONSTATE = stream_nyc_parking_ny.REGISTRATIONSTATE  ,
PLATETYPE = stream_nyc_parking_ny.PLATETYPE  ,
ISSUEDATE = stream_nyc_parking_ny.ISSUEDATE ,
VIOLATIONCODE  = stream_nyc_parking_ny.VIOLATIONCODE ,
VEHICLEBODYTYPE = stream_nyc_parking_ny.VEHICLEBODYTYPE  ,
VEHICLEMAKE = stream_nyc_parking_ny.VEHICLEMAKE  ,
ISSUINGAGENCY = stream_nyc_parking_ny.ISSUINGAGENCY 
when not matched and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE' and stream_nyc_parking_ny.REGISTRATIONSTATE = 'NY' then 
insert
(SUMMONSNUMBER,
    PLATEID  ,
REGISTRATIONSTATE  ,
PLATETYPE  ,
ISSUEDATE ,
VIOLATIONCODE ,
VEHICLEBODYTYPE  ,
VEHICLEMAKE  ,
ISSUINGAGENCY 
)
values (
stream_nyc_parking_ny.SUMMONSNUMBER,    
stream_nyc_parking_ny.PLATEID  ,
stream_nyc_parking_ny.REGISTRATIONSTATE  ,
stream_nyc_parking_ny.PLATETYPE  ,
stream_nyc_parking_ny.ISSUEDATE ,
stream_nyc_parking_ny.VIOLATIONCODE ,
stream_nyc_parking_ny.VEHICLEBODYTYPE  ,
stream_nyc_parking_ny.VEHICLEMAKE  ,
stream_nyc_parking_ny.ISSUINGAGENCY )

--task to check the stream data and load into target table 
create or replace task tsk_parking_nj
warehouse = 'WAREHOUSE_M'
schedule = '1 minute'
when system$stream_has_data('stream_nyc_parking_nj')
as
merge into TBLPARKINGNJ 
using stream_nyc_parking_nj
on stream_nyc_parking_nj.SUMMONSNUMBER = TBLPARKINGNJ.SUMMONSNUMBER 
when matched and metadata$action = 'INSERT' and metadata$isupdate = 'TRUE' and stream_nyc_parking_nj.REGISTRATIONSTATE = 'NJ' then
update set 
SUMMONSNUMBER = stream_nyc_parking_nj.SUMMONSNUMBER   ,
PLATEID = stream_nyc_parking_nj.PLATEID  ,
REGISTRATIONSTATE = stream_nyc_parking_nj.REGISTRATIONSTATE  ,
PLATETYPE = stream_nyc_parking_nj.PLATETYPE  ,
ISSUEDATE = stream_nyc_parking_nj.ISSUEDATE ,
VIOLATIONCODE  = stream_nyc_parking_nj.VIOLATIONCODE ,
VEHICLEBODYTYPE = stream_nyc_parking_nj.VEHICLEBODYTYPE  ,
VEHICLEMAKE = stream_nyc_parking_nj.VEHICLEMAKE  ,
ISSUINGAGENCY = stream_nyc_parking_nj.ISSUINGAGENCY 
when not matched and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE' and stream_nyc_parking_nj.REGISTRATIONSTATE = 'NJ' then 
insert
(SUMMONSNUMBER,PLATEID  ,
REGISTRATIONSTATE  ,
PLATETYPE  ,
ISSUEDATE ,
VIOLATIONCODE ,
VEHICLEBODYTYPE  ,
VEHICLEMAKE  ,
ISSUINGAGENCY 
)
values (
    stream_nyc_parking_nj.SUMMONSNUMBER ,
stream_nyc_parking_nj.PLATEID  ,
stream_nyc_parking_nj.REGISTRATIONSTATE  ,
stream_nyc_parking_nj.PLATETYPE  ,
stream_nyc_parking_nj.ISSUEDATE ,
stream_nyc_parking_nj.VIOLATIONCODE ,
stream_nyc_parking_nj.VEHICLEBODYTYPE  ,
stream_nyc_parking_nj.VEHICLEMAKE  ,
stream_nyc_parking_nj.ISSUINGAGENCY )


show tasks;

--resume the tasks 

alter task tsk_parking_ny resume;
alter task tsk_parking_nj resume;

--validate the history of tasks
select * from table(information_schema.task_history(task_name => 'tsk_parking_ny'));

select * from table(information_schema.task_history(task_name => 'tsk_parking_nj'));
