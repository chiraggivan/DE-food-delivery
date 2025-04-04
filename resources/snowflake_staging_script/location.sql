-- creating database and different schemas in snowflake
CREATE DATABASE IF NOT EXISTS sandbox;
USE DATABASE sandbox;
CREATE SCHEMA IF NOT EXISTS stage_sch;
CREATE SCHEMA IF NOT EXISTS clean_sch;
CREATE SCHEMA IF NOT EXISTS consumption_sch;
CREATE SCHEMA IF NOT EXISTS common;

USE SCHEMA stage_sch;

-- create file format to process the CSV file
create file format if not exists stage_sch.ff_csv 
    type = 'csv' 
    compression = 'auto' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\042' 
    null_if = ('\\N');

---------------------- NOTE : the above codes is only run once and not for any other tables-------------

-- create table in stage schema
create or replace table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- create stream on stage schema's location table
create or replace stream stage_sch.location_stm 
on table stage_sch.location
append_only = true
comment = 'this is the append-only stream object on location table that gets delta data based on changes';


---------------------- BELOW SQL SCRIPT for connecting snowflake to s3 ----------------------------

-- Create a storage integration to connect Snowflake to S3
CREATE OR REPLACE STORAGE INTEGRATION si_s3_to_snowflake
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::902651842113:role/RDStoS3role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://test.complete.food-delivery/');

-- Describe the integration to verify setup
DESC INTEGRATION si_s3_to_snowflake;

-- Update the storage integration with the correct IAM role
ALTER STORAGE INTEGRATION si_s3_to_snowflake
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::902651842113:role/SnowflakeToS3role';

-- Create an external stage to access the S3 bucket
CREATE OR REPLACE STAGE stg_s3_to_snowflake
    URL = 's3://test.complete.food-delivery/'
    STORAGE_INTEGRATION = si_s3_to_snowflake
    FILE_FORMAT = ff_csv;

-- Create a Snowpipe for automated ingestion into the location table
CREATE OR REPLACE PIPE pipe_s3_to_location
    AUTO_INGEST = TRUE
    AS
    COPY INTO location
    FROM (
        SELECT
            t.$1::text AS locationid,
            t.$2::text AS city,
            t.$3::text AS state,
            t.$4::text AS zipcode,
            t.$5::text AS activeflag,
            t.$6::text AS createddate,
            t.$7::text AS modifieddate,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5,
            current_timestamp AS _copy_data_ts
        FROM @stg_s3_to_snowflake/location/csv t
    )
    FILE_FORMAT = (format_name = ff_csv);

-- Retrieve the notification channel (SQS ARN) for Snowpipe
SHOW PIPES;


--- we only need to create snowpipe for the rest of the table 
--- all the sql script for snowpipe of other tables are in snowpipes.sql
