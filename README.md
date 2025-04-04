# Project: End-to-End Data Engineering Project for a Food Delivery Company

## Description

This project implements an end-to-end **data engineering pipeline** for a food delivery company, automating the flow of operational data from a relational database management system (RDBMS) to a data warehouse for advanced analytics. The pipeline extracts data from an **Amazon RDS** MySQL database, transfers it to an **AWS S3** bucket, loads it into **Snowflake** (a cloud-based data warehouse), and performs **ETL** (Extract, Transform, Load) processes within Snowflake to create a star schema model. The final output is a structured dataset ready for business intelligence and analytical reporting, enabling data analysts to derive insights into customer behavior, delivery trends, and operational performance.

The project leverages modern cloud technologies and best practices in data engineering to ensure scalability, efficiency, and reliability. It is divided into three distinct parts for better understanding and modularity:

1. Automated Data Transfer from Amazon RDS to S3 for a Food Delivery Company
2. Notification for Data Transfer from S3 to Snowflake for a Food Delivery Company
3. Tasks and Procedures to perform ETL process in snowflake and generate star schema model

## Project Part 1: Automated Data Transfer from Amazon RDS to S3 for a Food Delivery Company

### Overview
This project automates the transfer of operational data from an Amazon RDS MySQL database to an S3 bucket for a food delivery company. The `RDStoS3function` Lambda function extracts data incrementally from tables such as `location` and `customer`, processes it, and saves it as CSV files in S3. This pipeline ensures that data analysts have access to fresh data every 4 hours for monitoring business performance, such as customer growth and location-based trends. This is the first part of a larger end-to-end data engineering project that includes loading data into Snowflake and performing ETL transformations.

### Technologies Used
- **AWS Services**: AWS Lambda, EventBridge, Amazon RDS (MySQL), S3, Systems Manager (SSM), CloudWatch Logs (for logging)
- **Programming Language**: Python 3.11
- **Libraries**: `mysql.connector`, `pandas`, `boto3`, `logging`
- **IAM**: Configured roles and policies for Lambda to access RDS, S3, and SSM

### Architecture
The `RDStoS3function` Lambda function is triggered on a schedule (every 4 hours) via Amazon EventBridge. It retrieves RDS credentials securely from AWS Systems Manager Parameter Store, connects to an Amazon RDS MySQL database (`food_test_db`), and extracts data incrementally from tables like `location` and `customer` using SQL queries. The data is processed into CSV files using Pandas and uploaded to an S3 bucket (`test.complete.food-delivery`) under a folder structure like `{table}/csv/`. The last extracted timestamp is stored in S3 (e.g., `location/csv/last_extract.txt`) to enable incremental updates. The function logs its activity to CloudWatch Logs for monitoring and debugging.

### Architecture Diagram
![Diagram: Amazon EventBridge → RDStoS3function → System Manager → Amazon RDS → S3 → CloudWatch Logs](/resources/architecture-rds-to-s3.png)  

### Key Features
- Secure retrieval of RDS credentials using AWS Systems Manager Parameter Store.
- Incremental data extraction from Amazon RDS using `modifiedDate` and `createdDate` fields to process only new or updated records.
- Data storage in S3 as CSV files with a structured folder hierarchy (`{table}/csv/`).
- Comprehensive logging to CloudWatch Logs for monitoring and troubleshooting.

### Data Used
- **Source**: Amazon RDS MySQL database (`food_test_db`).
- **Tables**: The dataset includes 9 tables: `location`,`customer`,`restaurant`,`delivery_agent`, `customer_address`,`menu`,`orders`,`order_item` and `delivery`, which were created in RDS for testing purposes.
  - `location`: Contains information about delivery locations, such as `location_id`, `city`, `state`, `createdDate`, and `modifiedDate`.
  - `customer`: Contains customer information, such as `customer_id`, `name`, `email`, `createdDate`, and `modifiedDate`.
  - `restaurant`: Contains restaurant information, such as `restaurant_id`, ``, `name`, `cuisine_type`, `pricing_for_two`, `open_status`
  - `delivery_agent`: Contains delivery agent information, such as `delivery_agent_id`,`name`, `phone`, `vehicle_type`, `rating`
  - `customer_address`: Contains customer address information, such as `customer_address_id`,`customer_id`, `location_id`, `address_line`
  - `menu`: Contains menu information, such as `menu_id`,`restaurant_id`, `item_name`, `description`, `price`, `category`
  - `orders`: Contains orders information, such as `orders_id`, `customer_id`, `restaurant_id`, `order_date`, `total_amount`, `payment_method`
  - `order_item`: Contains order items information, such as `order_item_id`, `order_id`, `menu_id`, `quantity`, `price`, `sub_total`
  - `delivery`: Contains delivery information, such as `delivery_id`, `order_id`, `delivery_agent_id`, `delivery_status`, `est_time`
- **Data Type**: Synthetic data was used to test the system. For initial testing, a small dataset ranging from 5 to 35 rows per table was inserted, depending on the table and its business requirements.
- **Testing Process**: Additional rows were later inserted, and existing rows were updated to simulate real-world data changes. This ensured that the system could handle both inserts and updates, with each change recorded in a new CSV file for downstream consumption.
- **Format**: The data is extracted from RDS as a result of SQL queries, converted to CSV files using Pandas, and uploaded to S3 with a folder structure: `{table}/csv/` (e.g., `location/csv/location_data_20250402_120000.csv`).
- **Frequency**: Data is extracted every 4 hours, triggered by Amazon EventBridge.
- **Incremental Extraction**: Only new or updated records are extracted based on the `createdDate` and `modifiedDate` fields, using a timestamp stored in S3 (e.g., `location/csv/last_extract.txt`).

### Data Model
- **ERD**: The Entity-Relationship Diagram (ERD) below illustrates the structure of the tables in the RDS database.

![Diagram: ERD showcasing relationships between different tables](/resources/data-model-rds.png)

### Challenges Faced
- **Challenge 1**: Ensuring secure access to RDS credentials without hardcoding them in the Lambda function.
  - **Solution**: Used AWS Systems Manager Parameter Store to securely store and retrieve the RDS username and password.
- **Challenge 2**: Initially struggled with configuring the Lambda function to interact with multiple AWS services, such as retrieving credentials from Systems Manager Parameter Store and ensuring proper permissions to read/write to S3.
  - **Solution**: Configured the Lambda function’s IAM role with the necessary permissions (e.g., `ssm:GetParameter` for Systems Manager and `s3:GetObject`/`s3:PutObject` for S3) and tested the function with sample data to ensure proper connectivity.
- **Challenge 3**: Faced difficulties in implementing incremental data extraction to avoid reprocessing the entire dataset, which could lead to performance issues and increased costs.
  - **Solution**: Designed a timestamp-based mechanism by storing the last extracted timestamp in S3 (e.g., `location/csv/last_extract.txt`) and using it in SQL queries to extract only new or updated records. Added error handling for cases where the timestamp file was missing.

### Code Snippet
``` python

# Read the last extracted timestamp from S3
def get_last_extract_timestamp(s3_client, table):
        obj = s3_client.get_object(Bucket=TIMESTAMP_BUCKET, Key=f"{table}/csv/last_extract.txt")
        return obj['Body'].read().decode('utf-8').strip()
    
# Save the new last extracted timestamp to S3
def save_last_extract_timestamp(s3_client, timestamp, table):
        s3_client.put_object(Bucket=TIMESTAMP_BUCKET, Key=f"{table}/csv/last_extract.txt", Body=timestamp)
        
# Read the parameter values stored in system manager
def get_para(ssm_client, para_name):
        parameter = ssm_client.get_parameter(Name=para_name, WithDecryption=True)
        return parameter['Parameter']['Value']

# Connect to MySQL
conn = mysql.connector.connect(
    host="mysqldatabase.cb8ewcagm8cm.eu-north-1.rds.amazonaws.com",
    user=rds_username,  
    password=rds_password,  
    database="food_test_db"
)

# Retriving data
last_extract = get_last_extract_timestamp(s3_client, table)
query = f"""
SELECT *
FROM {table}
WHERE (modifiedDate > '{last_extract}'
    OR (modifiedDate IS NULL AND createdDate > '{last_extract}'))
"""
df = pd.read_sql(query, conn)

# Upload the CSV to S3
s3_key = f"{table}/csv/{table}_data_{timestamp}.csv"
s3_client.upload_file(local_file, TIMESTAMP_BUCKET, s3_key)


# Update the last extracted timestamp
latest_timestamp = df['modifiedDate'].fillna(df['createdDate']).max()
save_last_extract_timestamp(s3_client, str(latest_timestamp), table)
                    
```
### Python code : 
- Find the complete python code here: [rds-to-s3.py](/resources/rds_to_s3.py)

### Results and Impact
- Successfully automated the transfer of operational data (e.g., `location` and `customer` tables) from Amazon RDS to S3, enabling downstream analysis for a food delivery company.
- Provided data analysts with fresh data every 4 hours to monitor business performance, such as customer growth and location-based trends.
- Laid the foundation for the next steps in the pipeline: loading data into Snowflake and performing ETL transformations.

### Future Improvements
- Implement pagination or batch processing for large tables to handle high data volumes and avoid Lambda timeouts.
- Add data validation before uploading to S3 to ensure data quality (e.g., check for missing or invalid values).
- Use AWS Secrets Manager instead of Systems Manager Parameter Store for enhanced security of RDS credentials.

### Skills
- **Programming**: Python
- **Cloud**: AWS (Lambda, EvenTBridge, RDS, S3, Systems Manager, CloudWatch Logs, IAM)
- **Data Pipelines**: Incremental data extraction, data transfer

### Contact
- **Email**: [your-email@example.com]
- **Portfolio**: [Chirag Givan](https://chiraggivan.github.io/DataAnalyst/)
  
## Project Part 2: Data Transfer from S3 to Snowflake for a Food Delivery Company

### Overview
This project automates the transfer of operational data from an S3 bucket to Snowflake for a food delivery company. CSV files (e.g., `location_data_*.csv`, `customer_data_*.csv`) generated in the previous step (RDS to S3) are ingested into corresponding tables in Snowflake (like `location` and `customer`) using Snowflake’s `COPY INTO` command and Snowpipe for automated ingestion. This enables data analysts to perform advanced analytics on the food delivery company’s data, such as customer behavior and location-based trends, using Snowflake’s data warehousing capabilities. This is the second part of a larger end-to-end data engineering project that includes extracting data from RDS to S3 and performing ETL transformations in Snowflake.

### Technologies Used
- **AWS Services**: S3, SQS (for event notifications), IAM
- **Snowflake**: Snowflake Data Cloud (for data warehousing), `COPY INTO` command, Snowpipe (for automated ingestion), Storage Integration
- **IAM**: Configured roles for Snowflake to access S3

### Architecture
The architecture leverages Snowflake’s Snowpipe for automated data ingestion. An S3 bucket (`test.complete.food-delivery`) stores CSV files generated from the RDS to S3 pipeline. An S3 event notification (using SQS) is triggered whenever a new file is added to the bucket. Snowpipe detects the event via the SQS queue and automatically ingests the new CSV files into Snowflake tables (e.g., `location`) using a predefined pipe (`rds_to_s3_snowpipe`). Snowflake accesses the S3 bucket through an external stage (`rds_to_s3_stage`) and a storage integration (`rds_to_s3_int`) with the appropriate IAM role (`SnowflakeToS3role`).

![Architecture Diagram](/resources/architecture-s3-to-snowflake.png)

### Key Features
- Automated data ingestion from S3 to Snowflake using Snowpipe, triggered by S3 event notifications.
- Secure access to S3 using a Snowflake storage integration and IAM role.
- Support for incremental data loading by processing only new CSV files in S3.
- Metadata tracking in Snowflake tables (e.g., file name, load timestamp) for auditing and debugging.

### Challenges Faced
- **Challenge 1**: Connecting Snowflake to S3 and setting up the storage integration.
  - **Solution**: Created a storage integration (`rds_to_s3_int`) in Snowflake with the correct IAM role (`SnowflakeToS3role`) and updated the role’s trust policy to allow Snowflake to assume it. Used the `DESC INTEGRATION` command to verify the setup and ensure the correct ARN was applied.
- **Challenge 2**: Configuring the complete architecture for automated ingestion, including S3 event notifications and Snowpipe.
  - **Solution**: Set up an S3 event notification with an SQS queue to trigger Snowpipe whenever new files are added to the bucket. Used the `SHOW PIPES` command to retrieve the `notification_channel` (SQS ARN) and configured the S3 event notification to send events to this queue.

### Code Snippet
The following Snowflake SQL commands set up the storage integration, stage, and Snowpipe for automated ingestion of CSV files from S3 into the `location` table:

```sql
-- Create a storage integration to connect Snowflake to S3
CREATE OR REPLACE STORAGE INTEGRATION rds_to_s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::902651842113:role/RDStoS3role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://test.complete.food-delivery/');

-- Create an external stage to access the S3 bucket
CREATE OR REPLACE STAGE rds_to_s3_stage
    URL = 's3://test.complete.food-delivery/'
    STORAGE_INTEGRATION = rds_to_s3_int
    FILE_FORMAT = ff_csv;

-- Create a Snowpipe for automated ingestion into the location table
CREATE OR REPLACE PIPE rds_to_s3_snowpipe
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
        FROM @rds_to_s3_stage/location/csv t
    )
    FILE_FORMAT = (format_name = ff_csv);

```
-- Note: 
    -  An S3 event notification (snowpipe-event) was created in AWS to trigger Snowpipe via the SQS queue (using the notification_channel ARN).
    -  Created event notifications for every snowpipe created. 

### SQL code : 
- Find the complete snowflake SQL code here: [snowflake_staging_script.sql](/resources/snowflake_staging_script/location.sql)

  *(Note: The above sql file only shows the sql code for location entity. Other sql code are in the folder called snowflake_staging_script)*

### Results and Impact
- Successfully automated the ingestion of operational data from S3 into Snowflake, enabling real-time analytics for a food delivery company.
- Ingested CSV files (e.g., `location_data_*.csv`, `customer_data_*.csv`) into Snowflake tables (`location`, `customer`) whenever new files are added to S3.
- Enabled data analysts to query the `location` and `customer` tables in Snowflake for insights into customer behavior and location-based trends.
- Reduced latency between data availability in S3 and Snowflake by using Snowpipe for near-real-time ingestion.

### Future Improvements
- Implement error handling and retry mechanisms in Snowpipe to manage failed loads (e.g., due to malformed CSV files).
- Add data validation in Snowflake to ensure data quality after ingestion (e.g., check for duplicates or missing values).
- Use Snowflake’s task and notification features to monitor Snowpipe performance and alert on failures.

### Skills
- **Programming**: Python, SQL
- **Cloud**: AWS (Lambda, RDS, S3, Systems Manager, Amazon EventBridge, CloudWatch Logs, SQS, IAM), Snowflake (Snowpipe, Storage Integration)
- **Data Pipelines**: Incremental data extraction, automated data ingestion, data transfer
