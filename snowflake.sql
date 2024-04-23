-- CREATE A TABLE
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

-- CHECK THE CONTENT
list @citibike_trips;

-- CREATE FILE FORMAT
create or replace file format csv type = 'csv'
    compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
    skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
    error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
    date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for CITIBIKE_WS1 zero to snowflake';

-- VERIFY FILE FORMAT IS CREATED
show file formats in database citibike;

-- TO LOAD STAGED DATA INTO THE TABLE
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*';

-- CLEAR THE TABLE OF ALL DATA AND METADATA
truncate table trips;

-- VERIFY TABLE IS CLEAR
select * from trips limit 10;

-- Change the warehouse size to large using the following ALTER WAREHOUSE
alter warehouse compute_wh set warehouse_size='large';

-- load data with large warehouse
show warehouses;

-- load the same data again
copy into trips from @citibike_trips
file_format = CSV;

select * from trips limit 20;

-- For each hour, show number of trips, average trip duration, and average trip distance
select EXTRACT(HOUR from STARTTIME), 
count(*) as "num trips", 
avg(tripduration)/60 as "avg duration(mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance(km)"
from trips
group by EXTRACT(HOUR from STARTTIME)
order by EXTRACT(HOUR from STARTTIME);


-- Which months are the busiest
select EXTRACT(MONTH from STARTTIME) as "month of trip", COUNT(*) as "num trips"
from trips
group by 1
order by 2 desc;

-- Create a dev table clone of the trips table
create table trips_dev clone trips;



-- Create a database named WEATHER to use for storing semi-structured JSON data
create database weather;

-- Execute USE commands to set the worksheet context appropriately
use role accountadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- Create a table for loading JSON data
create table json_weather_data (v variant);

-- Create a stage that points to the bucket where the semi-structured JSON data is stored on AWS S3
create stage nyc_weather
url= 's3://snowflake-workshop-lab/zero-weather-nyc';

list @nyc_weather;

-- Load data from S3 bucket into the json_weather_data table
copy into json_weather_data
from @nyc_weather
    file_format = (type = json strip_outer_array = true);

select * from json_weather_data limit 10;

-- Create a view that will put structure onto the semi-structured data. The 72502 value for station_id corresponds to Newark Airport, closest station that has weather conditions for the whole period.
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from json_weather_data
where station_id = '72502';

-- Verify the view
select * from json_weather_data_view
where date_trunc('month', observation_time) = '2018-01-01'
limit 20;

-- Join JSON weather data to citibike.public.trips data to count no. of trips associated with certain weather conditions
select weather_conditions as conditions, count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1
order by 2 desc;

-- The initial goal was to determine if there was any correlation between the number of bike rides and the weather by analyzing both ridership and weather data. Per the results of this query, we have a clear answer. As one would imagine, the number of trips is significantly higher when the weather is good!

--  Restore data objects that have been accidentally or intentionally deleted
drop table json_weather_data;

-- verify table is dropped
select * from json_weather_data limit 10;

-- Restore the table
undrop table json_weather_data;

-- verify table is undropped
select * from json_weather_data limit 10;


-- Let's roll back the TRIPS table in the CITIBIKE database to a previous state to fix an unintentional DML error that replaces all the station names in the table with the word "oops".
-- switch worksheet to the proper context
use role accountadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- replace all of the station names in the table with the word "oops":
update trips set start_station_name = 'oops';

select start_station_name as "station", count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

-- recreate the table with the correct station names.
create or replace table trips as
(select * from trips before (statement => $query_id));

-- Run the previous query again to verify that the station names have been restored.
select start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;



-- Working with Roles, Account Admin, & Account Usage
-- Create a New Role and Add a User
create role junior_dba;
grant role junior_dba to user RASHISAKI;

use role junior_dba;

-- switching back to ADMIN role and grant usage privileges to COMPUTE_WH warehouse.
use role accountadmin;
grant usage on warehouse compute_wh to role junior_dba;

-- Switch back to the JUNIOR_DBA role. You should be able to use COMPUTE_WH now.
use role junior_dba;
use warehouse compute_wh;

-- Switch back to the ACCOUNTADMIN role and grant the JUNIOR_DBA the USAGE privilege required to view and use the CITIBIKE and WEATHER databases:
use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

use role junior_dba;