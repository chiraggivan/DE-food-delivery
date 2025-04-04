
-- Create a Snowpipe for automated ingestion into the customer table
CREATE OR REPLACE PIPE pipe_s3_to_customer
    AUTO_INGEST = TRUE
    AS
    COPY INTO customer
    FROM (
    select 
        t.$1::text as customer_id,
        t.$2::text as first_name,
        t.$3::text as last_name,
        t.$4::text as mobile,
        t.$5::text as email,
        t.$6::text as loginbyusing,
        t.$7::text as gender,
        t.$8::text as dob,
        t.$9::text as anniversary,
        t.$10::text as preferences,
        t.$11::text as active_flag,
        t.$12:text as createdDate,
        t.$13::text as modifiedDate,
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
        t.$1::text as restaurant_id,
        t.$2::text as name,
        t.$3::text as cuisine_type,
        t.$4::text as pricing_for_two,
        t.$5::text as phone,
        t.$6::text as manager_name,
        t.$7::text as operating_hours,
        t.$8::text as address_line_1,
        t.$9::text as address_line_2,
        t.$10::text as address_line_3,
        t.$11::text as landmark,
        t.$12::text as location_id,
        t.$13::text as open_status,
        t.$14::text as longitude,
        t.$15::text as latitude,
        t.$16::text as active_flag,
        t.$17::text as createdDate,
        t.$18::text as modifiedDate,
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
        t.$1::text as menu_id,
        t.$2::text as restaurant_id,
        t.$3::text as item_name,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as item_type,
        t.$9::text as active_flag,
        t.$10::text as createdDate,
        t.$11::text as modifiedDate,
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
        t.$1::text as customer_address_id,
        t.$2::text as customer_id,
        t.$3::text as address_line_1,
        t.$4::text as address_line_2,
        t.$5::text as address_line_3,
        t.$6::text as landmark,
        t.$7::text as location_id,
        t.$4::text as address_type,
        t.$5::text as primary_flag,
        t.$6::text as latitude,
        t.$7::text as longitude,
        t.$4::text as active_flag,
        t.$5::text as createdDate,
        t.$6::text as modifiedDate,
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
        t.$1::text as delivery_agent_id,
        t.$2::text as first_name,
        t.$3::text as last_name,
        t.$4::text as dob,
        t.$5::text as address_line_1,
        t.$6::text as address_line_2,
        t.$7::text as address_line_3,
        t.$8::text as landmark,
        t.$9::text as location_id,
        t.$10::text as phone,
        t.$11::text as gender,
        t.$12::text as vehicle_type,
        t.$13::text as verified,
        t.$14::text as status,
        t.$15::text as rating,
        t.$16::text as driver_license,
        t.$17::text as active_flag,
        t.$18::text as createdDate,
        t.$19::text as modifiedDate,
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
        t.$1::text as order_id,
        t.$2::text as customer_id,
        t.$3::text as restaurant_id,
        t.$4::text as order_date,
        t.$5::text as total_amount,
        t.$6::text as status,
        t.$7::text as payment_method,
        t.$8::text as createdDate,
        t.$9::text as modifiedDate,
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
        t.$1::text as order_item_id,
        t.$2::text as order_id,
        t.$3::text as menu_id,
        t.$4::text as quantity,
        t.$5::text as price,
        t.$6::text as sub_total,
        t.$7::text as createdDate,
        t.$8::text as modifiedDate,
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
        t.$1::text as delivery_id,
        t.$2::text as order_id,
        t.$3::text as delivery_agent_id,
        t.$4::text as delivery_status,
        t.$5::text as est_time,
        t.$6::text as customer_address_id,
        t.$7::text as delivery_date,
        t.$6::text as createdDate,
        t.$7::text as modifiedDate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stg_s3_to_snowflake/delivery/csv t
)
    FILE_FORMAT = (format_name = ff_csv);
