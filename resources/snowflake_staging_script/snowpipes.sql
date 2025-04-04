
-- Create a Snowpipe for automated ingestion into the customer table
CREATE OR REPLACE PIPE pipe_s3_to_customer
    AUTO_INGEST = TRUE
    AS
    COPY INTO customer
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/customer/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the restaurant table
CREATE OR REPLACE PIPE pipe_s3_to_restaurant
    AUTO_INGEST = TRUE
    AS
    COPY INTO restaurant
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/restaurant/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the menu table
CREATE OR REPLACE PIPE pipe_s3_to_menu
    AUTO_INGEST = TRUE
    AS
    COPY INTO menu
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/menu/csv t
)
    FILE_FORMAT = (format_name = ff_csv);

-- Create a Snowpipe for automated ingestion into the customer_address table
CREATE OR REPLACE PIPE pipe_s3_to_customer_address
    AUTO_INGEST = TRUE
    AS
    COPY INTO customer_address
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/customer_address/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the delivery_agent table
CREATE OR REPLACE PIPE pipe_s3_to_delivery_agent
    AUTO_INGEST = TRUE
    AS
    COPY INTO delivery_agent
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/delivery_agent/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the orders table
CREATE OR REPLACE PIPE pipe_s3_to_orders
    AUTO_INGEST = TRUE
    AS
    COPY INTO orders
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/orders/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the order_item table
CREATE OR REPLACE PIPE pipe_s3_to_order_item
    AUTO_INGEST = TRUE
    AS
    COPY INTO order_item
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/order_item/csv t
)
    FILE_FORMAT = (format_name = ff_csv);


-- Create a Snowpipe for automated ingestion into the delivery table
CREATE OR REPLACE PIPE pipe_s3_to_delivery
    AUTO_INGEST = TRUE
    AS
    COPY INTO delivery
    FROM (
    select 
        t.$1::text as ,
        t.$2::text as ,
        t.$3::text as ,
        t.$4::text as ,
        t.$5::text as ,
        t.$6::text as ,
        t.$7::text as ,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/delivery/csv t
)
    FILE_FORMAT = (format_name = ff_csv);