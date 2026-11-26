-- Load one day's staged CSVs from S3 into RAW.RAW_LOGS.
-- NOT RUN: needs Snowflake + S3. Same rules as the local loader:
-- values unchanged, a file's earlier rows replaced (never duplicated), all in
-- one transaction. Replace <YYYY-MM-DD> with the ingest date first (COPY
-- needs a literal path; it cannot take a variable).

USE ROLE ROBOT_TRANSFORMER;
USE WAREHOUSE ROBOT_WH;
USE SCHEMA ROBOT_LOGS.RAW;

CREATE OR REPLACE TEMPORARY TABLE INCOMING LIKE RAW_LOGS;

-- Columns are matched by header name, ignoring case: the CSV header says
-- source_file while the unquoted table column is stored as SOURCE_FILE. (The 24
-- source names stay distinct when case is ignored.) A case-sensitive match
-- would silently load SOURCE_FILE as NULL and break the per-file DELETE.
-- FORCE = TRUE re-reads files COPY has seen before; the DELETE below keeps that
-- idempotent.
COPY INTO INCOMING
  FROM @ROBOT_STAGE/ingest_date=<YYYY-MM-DD>/
  PATTERN = '.*[.]csv'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FORCE = TRUE
  ON_ERROR = ABORT_STATEMENT;

-- Refuse to continue if any row came in without its file name.
SELECT IFF(COUNT_IF(SOURCE_FILE IS NULL) = 0, 'ok',
           1 / 0) AS source_file_check  -- division by zero aborts the script
FROM INCOMING;

BEGIN;
DELETE FROM RAW_LOGS WHERE SOURCE_FILE IN (SELECT DISTINCT SOURCE_FILE FROM INCOMING);
INSERT INTO RAW_LOGS
  SELECT * EXCLUDE (LOADED_AT), CURRENT_TIMESTAMP() AS LOADED_AT FROM INCOMING;
INSERT INTO LOAD_AUDIT
  SELECT SOURCE_FILE, NULL, COUNT(*), NULL, CURRENT_TIMESTAMP(), 'loaded'
  FROM INCOMING GROUP BY SOURCE_FILE;
COMMIT;
