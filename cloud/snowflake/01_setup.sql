-- Snowflake setup for the robot log pipeline.
-- NOT RUN: needs a Snowflake account (and an AWS account for the S3 stage).
-- Run once, in order, as a role allowed to create these objects
-- (ACCOUNTADMIN for the storage integration; SYSADMIN for the rest).
-- Replace the three <...> placeholders first. No secrets go in this file.

-- 1. Compute: the smallest warehouse, paused after 60 s idle to save credits.
CREATE WAREHOUSE IF NOT EXISTS ROBOT_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- 2. Storage: raw data and dbt output in separate schemas.
CREATE DATABASE IF NOT EXISTS ROBOT_LOGS;
CREATE SCHEMA IF NOT EXISTS ROBOT_LOGS.RAW;
CREATE SCHEMA IF NOT EXISTS ROBOT_LOGS.ANALYTICS;

-- 3. Access: one role for dbt, one key-pair user (no password).
CREATE ROLE IF NOT EXISTS ROBOT_TRANSFORMER;
GRANT USAGE ON WAREHOUSE ROBOT_WH TO ROLE ROBOT_TRANSFORMER;
GRANT USAGE ON DATABASE ROBOT_LOGS TO ROLE ROBOT_TRANSFORMER;
GRANT USAGE, CREATE TABLE ON SCHEMA ROBOT_LOGS.RAW TO ROLE ROBOT_TRANSFORMER;
GRANT ALL ON SCHEMA ROBOT_LOGS.ANALYTICS TO ROLE ROBOT_TRANSFORMER;
CREATE USER IF NOT EXISTS ROBOT_DBT
  TYPE = SERVICE
  DEFAULT_ROLE = ROBOT_TRANSFORMER
  DEFAULT_WAREHOUSE = ROBOT_WH
  RSA_PUBLIC_KEY = '<contents of rsa_key.pub without the BEGIN/END lines>';
GRANT ROLE ROBOT_TRANSFORMER TO USER ROBOT_DBT;

-- 4. Raw table: column names exactly as in the source file (quoted, so case
--    and the trailing space in "cycle " are kept), plus the loader's columns.
CREATE TABLE IF NOT EXISTS ROBOT_LOGS.RAW.RAW_LOGS (
  "Num" NUMBER, "Timestamp" VARCHAR,
  "Current_J0" FLOAT, "Temperature_T0" FLOAT,
  "Current_J1" FLOAT, "Temperature_J1" FLOAT,
  "Current_J2" FLOAT, "Temperature_J2" FLOAT,
  "Current_J3" FLOAT, "Temperature_J3" FLOAT,
  "Current_J4" FLOAT, "Temperature_J4" FLOAT,
  "Current_J5" FLOAT, "Temperature_J5" FLOAT,
  "Speed_J0" FLOAT, "Speed_J1" FLOAT, "Speed_J2" FLOAT,
  "Speed_J3" FLOAT, "Speed_J4" FLOAT, "Speed_J5" FLOAT,
  "Tool_current" FLOAT, "cycle " NUMBER,
  "Robot_ProtectiveStop" FLOAT, "grip_lost" BOOLEAN,
  SOURCE_FILE VARCHAR,
  LOADED_AT TIMESTAMP_TZ
);
CREATE TABLE IF NOT EXISTS ROBOT_LOGS.RAW.LOAD_AUDIT (
  SOURCE_FILE VARCHAR, SHA256 VARCHAR, ROWS_LOADED NUMBER,
  ROWS_REPLACED NUMBER, LOADED_AT TIMESTAMP_TZ, ACTION VARCHAR
);

-- 5. S3 access without keys: Snowflake assumes an IAM role you create
--    (see cloud/README.md, "S3 -> Snowflake").
CREATE STORAGE INTEGRATION IF NOT EXISTS ROBOT_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<arn:aws:iam::123456789012:role/robot-snowflake-read>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>/staged/robot_logs/');
-- Then: DESC INTEGRATION ROBOT_S3_INT;  copy STORAGE_AWS_IAM_USER_ARN and
-- STORAGE_AWS_EXTERNAL_ID into the IAM role's trust policy.

CREATE FILE FORMAT IF NOT EXISTS ROBOT_LOGS.RAW.ROBOT_CSV
  TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE NULL_IF = ('');

CREATE STAGE IF NOT EXISTS ROBOT_LOGS.RAW.ROBOT_STAGE
  STORAGE_INTEGRATION = ROBOT_S3_INT
  URL = 's3://<your-bucket>/staged/robot_logs/'
  FILE_FORMAT = ROBOT_LOGS.RAW.ROBOT_CSV;
GRANT USAGE ON INTEGRATION ROBOT_S3_INT TO ROLE ROBOT_TRANSFORMER;
GRANT USAGE ON STAGE ROBOT_LOGS.RAW.ROBOT_STAGE TO ROLE ROBOT_TRANSFORMER;
GRANT USAGE ON FILE FORMAT ROBOT_LOGS.RAW.ROBOT_CSV TO ROLE ROBOT_TRANSFORMER;
