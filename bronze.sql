-- ########
-- Set up
-- ########

CREATE DATABASE jc_demo;
CREATE SCHEMA jc_demo.bronze;

USE DATABASE JC_DEMO;
USE SCHEMA BRONZE;

JC_DEMO.BRONZECREATE OR REPLACE WAREHOUSE car_warehouse
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60 -- if no query runs for 60 seconds, compute turns off
  AUTO_RESUME = TRUE; -- query wakes it up

USE warehouse car_warehouse;

CREATE OR REPLACE FILE FORMAT bronze.csv_fileFormat
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--DROP STAGE bronze.car_stage
CREATE OR REPLACE STAGE bronze.car_stage
  URL = 's3://jc-car-analytic-demo-2026/bronze/'
  FILE_FORMAT = bronze.csv_fileFormat;

CREATE OR REPLACE STORAGE INTEGRATION snowflake_jc_demo_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '' 
  STORAGE_ALLOWED_LOCATIONS = ('');

DESC INTEGRATION snowflake_jc_demo_integration;
  
CREATE OR REPLACE STAGE bronze.car_stage
  URL = ''
  STORAGE_INTEGRATION = snowflake_jc_demo_integration;

-- Show all CSVs in the stage
  LIST @bronze.car_stage;


CREATE OR REPLACE FILE FORMAT bronze.csv_fileFormat
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('', 'NULL', 'null')
FIELD_OPTIONALLY_ENCLOSED_BY = '"';




SELECT * from bronze.car_listings_raw limit 5

--USE ROLE ACCOUNTADMIN;
--USE WAREHOUSE COMPUTE_WH;
--USE DATABASE car_analytics;
--USE SCHEMA bronze;

--CREATE OR REPLACE DATABASE car_analytics;

--USE DATABASE car_analytics;



-- ############
--CREATE tables
-- ############

CREATE OR REPLACE TABLE car_listings_raw (
    listing_id STRING,
    vin STRING,
    make STRING,
    model STRING,
    year INT,
    price NUMBER(10,2),
    mileage INT,
    dealer_id STRING,
    city STRING,
    state STRING,
    listed_date DATE,
    source_file_name STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    
);

CREATE OR REPLACE TABLE bronze.dealers_raw (
  dealer_id STRING,
  dealer_name STRING,
  dealer_type STRING,
  city STRING,
  state STRING,
  source_file_name STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE bronze.sales_raw (
  listing_id STRING,
  sold_price NUMBER,
  sold_date DATE,
  source_file_name STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);


-- ####################################
--LOAD from staging into bronze tables
-- ####################################
COPY INTO bronze.car_listings_raw
(listing_id, vin, make, model, year, price, mileage, dealer_id, city, state, listed_date, source_file_name)
FROM (
SELECT
  t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9,  t.$10, t.$11, metadata$filename
FROM @bronze.car_stage/car_listings.csv (FILE_FORMAT => bronze.csv_fileFormat) t
)

--SELECT top 10 * from bronze.car_listings_raw


COPY INTO bronze.dealers_raw
  (dealer_id, dealer_name, dealer_type, city, state,source_file_name)
FROM (
  SELECT
    s.$1,
    s.$2,
    s.$3,
    s.$4,
    s.$5,
    metadata$filename
  FROM @bronze.car_stage/dealers.csv
  (FILE_FORMAT => bronze.csv_fileFormat) s
);

COPY INTO bronze.sales_raw
  (listing_id, sold_price, sold_date,source_file_name)
FROM (
  SELECT
    s.$1,
    s.$2,
    s.$3,
    metadata$filename
  FROM @bronze.car_stage/sales.csv
  (FILE_FORMAT => bronze.csv_fileFormat) s
);



SELECT COUNT(1) FROM bronze.car_listings_raw;
SELECT COUNT(1) FROM bronze.dealers_raw;
SELECT COUNT(1) FROM bronze.sales_raw;

-- Bronze layer is good. Data are landed






-- ##############################
-- exmaple of creating a task
-- ##############################

CREATE TASK jc_demo.bronze.bronze_load_task
WAREHOUSE= car_warehouse
SCHEDULE='USING CRON 0 0 * * * UTC'
AS

COPY INTO bronze.car_listings_raw
FROM (
  SELECT
    t.$1  AS listing_id,
    t.$2  AS dealer_id,
    t.$3  AS make,
    t.$4  AS model,
    t.$5  AS year,
    t.$6  AS price,
    t.$7  AS mileage,
    t.$8  AS city,
    t.$9  AS state,
    t.$10 AS listed_date,
    t.$11 AS status
  FROM @bronze.car_stage/car_listings.csv
  (FILE_FORMAT => bronze.csv_fileFormat) t
);




