-- =============================================================================
-- Streaming side of the reconciliation: read Glue streaming output in place.
-- Run as ACCOUNTADMIN (or a role that owns the schema). Idempotent.
-- =============================================================================
-- Replace <INTEGRATION_NAME> with the storage integration that
-- mobile_coverage_stage already uses:  DESC STAGE mobile_coverage.staging.mobile_coverage_stage;

CREATE STAGE IF NOT EXISTS mobile_coverage.staging.streaming_stage
  URL = 's3://roy-projects/mobile-network-lab/aggregations/'
  STORAGE_INTEGRATION = <INTEGRATION_NAME>
  FILE_FORMAT = (TYPE = PARQUET);

-- External table: no COPY, no duplicates on rerun. Partition values come from
-- the folder names (Spark strips them from the file contents), the rest from
-- the Parquet columns. PATTERN skips Spark's _spark_metadata JSON log.
CREATE OR REPLACE EXTERNAL TABLE mobile_coverage.staging.ext_streaming_status_count (
    window_start   TIMESTAMP_NTZ AS (VALUE:window_start::TIMESTAMP_NTZ),
    window_end     TIMESTAMP_NTZ AS (VALUE:window_end::TIMESTAMP_NTZ),
    status         STRING        AS (VALUE:status::STRING),
    status_count   NUMBER        AS (VALUE:status_count::NUMBER),
    partition_hour STRING        AS (REGEXP_SUBSTR(METADATA$FILENAME, 'partition_hour=([0-9]+)', 1, 1, 'e', 1)),
    postal_code    STRING        AS (REGEXP_SUBSTR(METADATA$FILENAME, 'postal_code=([^/]+)', 1, 1, 'e', 1))
)
LOCATION = @mobile_coverage.staging.streaming_stage/status_count/
PATTERN = '.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

GRANT USAGE ON STAGE mobile_coverage.staging.streaming_stage TO ROLE MOBILE_COVERAGE_ENGINEER;
GRANT SELECT ON EXTERNAL TABLE mobile_coverage.staging.ext_streaming_status_count TO ROLE MOBILE_COVERAGE_ENGINEER;
-- Lets the DAG run ALTER EXTERNAL TABLE ... REFRESH as the Airflow role:
GRANT OWNERSHIP ON EXTERNAL TABLE mobile_coverage.staging.ext_streaming_status_count
  TO ROLE MOBILE_COVERAGE_ENGINEER COPY CURRENT GRANTS;

-- Sanity check: must match Athena (2026-08-11: 250, 2026-09-29: 250, 2026-10-06: 249)
USE ROLE MOBILE_COVERAGE_ENGINEER;
ALTER EXTERNAL TABLE mobile_coverage.staging.ext_streaming_status_count REFRESH;
SELECT TO_DATE(window_start) AS event_date, COUNT(*) AS rows, SUM(status_count) AS events
FROM mobile_coverage.staging.ext_streaming_status_count
GROUP BY 1 ORDER BY 1;