"""
==============================================================================
Mobile Coverage — Daily Batch Reconciliation Pipeline
==============================================================================

WHAT THIS DAG DOES:
    Runs nightly to independently recompute the previous day's mobile network
    aggregates from raw events (bronze S3 layer), model them into a warehouse
    (Snowflake gold layer), and reconcile against the streaming pipeline's
    outputs. Alerts on any divergence beyond a tolerance threshold.

WHY IT EXISTS:
    Streaming pipelines are fast but harder to guarantee correctness for.
    Batch pipelines are slower but easier to verify. Running both and
    comparing their outputs is the Lambda Architecture pattern — the
    canonical production design for high-stakes analytical data.

RUN SCHEDULE:
    Nightly at 2 AM UTC, processing the previous full UTC day.
    catchup=False → does not backfill missed runs on deploy.

TASK FLOW:
    wait_for_bronze
        ↓
    spark_batch_aggregate
        ↓
    load_to_snowflake
        ↓
    dbt (run + test, one Airflow task per model)
        ↓
    reconcile
        ↓
    refresh_dashboard_cache
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor #Used by task: wait_for_bronze
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator #Used by task: spark_batch_aggregate
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator #Load to snowflake task

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"

# -----------------------------------------------------------------------------
# DAG-level defaults
# -----------------------------------------------------------------------------
# WHY THESE DEFAULTS?
#
# owner:            who to contact when this breaks (visible in Airflow UI)
# retries:          transient failures (network blips, S3 throttling) recover
#                   on retry; permanent failures fail all N retries and alert
# retry_delay:      small backoff between retries — 5 min is standard for
#                   hourly-ish jobs; production would use exponential backoff
# depends_on_past:  False means today's run doesn't wait for yesterday's
#                   success. For reconciliation this is correct — each day's
#                   data is independent.
# email_on_failure: leave False for now; wire up SNS/SMTP once alerting is
#                   properly configured

default_args = {
    "owner": "roy",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
# schedule "0 2 * * *" = "at 2:00 AM every day" (cron syntax).
# start_date is set to a fixed date in the past so triggering manually works.
# catchup=False → do not backfill missed runs from start_date to now.
# max_active_runs=1 → never run two instances of this DAG concurrently.
# tags → filterable labels in the UI.

with DAG(
    dag_id="mobile_coverage_daily_batch",
    description="Nightly batch reconciliation of streaming aggregates",
    default_args=default_args,
    start_date=datetime(2026, 8, 15),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["mobile-coverage", "batch", "reconciliation"],
    doc_md=__doc__,  # renders this file's top docstring in the DAG's UI page
) as dag:

    # -------------------------------------------------------------------------
    # 1. Wait for the previous day's bronze partition to be complete
    # -------------------------------------------------------------------------
    # Firehose has no _SUCCESS marker, so completeness is inferred by
    # checking for the LAST hour partition of the day (hour=23). Combined
    # with the 2 AM UTC schedule (2hr cushion past midnight for Firehose's
    # buffer flush to land), this is a reasonably safe proxy for "day complete."

    wait_for_bronze = S3KeySensor(
    task_id="wait_for_bronze",
    bucket_name="roy-projects",
    bucket_key=(
        "mobile-network-lab/bronze/raw-events/"
        "year={{ data_interval_start.strftime('%Y') }}/"
        "month={{ data_interval_start.strftime('%m') }}/"
        "day={{ data_interval_start.strftime('%d') }}/"
        "hour=23/*.gz"
    ),
    wildcard_match=True,
    aws_conn_id="aws_default",
    deferrable=True,
    timeout=60 * 60 * 3,      # 3 hours — generous ceiling before alerting/failing
    poke_interval=60 * 5,     # only relevant in non-deferrable fallback; harmless here
)

    # -------------------------------------------------------------------------
    # 2. Spark batch aggregation on EMR Serverless
    # -------------------------------------------------------------------------
    # Reads bronze JSON, computes the same aggregations the streaming pipeline
    # produces (signal by operator, GPS by provider, status counts), writes
    # Parquet to a silver_batch S3 prefix.
    spark_batch_aggregate = EmrServerlessStartJobOperator(
        task_id="spark_batch_aggregate",
        application_id="00g8e5q4kn2k7909",
        execution_role_arn="arn:aws:iam::707491457732:role/mobile-coverage-emr-serverless-role",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://roy-projects/mobile-network-lab/emr-scripts/batch_aggregate.py",
                "entryPointArguments": ["{{ ds }}"],
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://roy-projects/mobile-network-lab/emr-logs/"
                }
            }
        },
        aws_conn_id="aws_default",
        deferrable=True,
        doc_md="""
        ### spark_batch_aggregate
        Runs `batch_aggregate.py` on EMR Serverless — reads yesterday's full
        bronze partition, computes signal/GPS/status aggregates (mirroring
        `streaming/glue_streaming.py`'s logic minus windowing), writes Parquet
        to `silver_batch/{aggregate}/date={{ ds }}/`.
        """,
)
    # -------------------------------------------------------------------------
    # 3. Load silver_batch into Snowflake staging
    # -------------------------------------------------------------------------
    # Uses Snowflake's COPY INTO from S3 via an external stage.
    # This is the boundary where data leaves the AWS lake and enters
    # the Snowflake warehouse for dbt to model.
    load_to_snowflake = SQLExecuteQueryOperator(
    task_id="load_to_snowflake",
    conn_id="snowflake_default",
    sql=[
        """
        COPY INTO mobile_coverage.staging.raw_signal_strength_by_operator
            (process_date, partition_hour, postal_code, operator, average_signal, _file_name)
        FROM (
            SELECT
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e', 1)),
                REGEXP_SUBSTR(METADATA$FILENAME, 'partition_hour=([0-9]+)', 1, 1, 'e', 1),
                REGEXP_SUBSTR(METADATA$FILENAME, 'postal_code=([^/]+)', 1, 1, 'e', 1),
                $1:"operator"::STRING,
                $1:"average_signal"::FLOAT,
                METADATA$FILENAME
            FROM @mobile_coverage.staging.silver_batch_stage/signal_strength_by_operator/date={{ ds }}/
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\\\.parquet'
        ON_ERROR = 'ABORT_STATEMENT';
        """,
        """
        COPY INTO mobile_coverage.staging.raw_gps_precision_by_provider
            (process_date, partition_hour, provider, average_precision, average_satellites, _file_name)
        FROM (
            SELECT
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e', 1)),
                REGEXP_SUBSTR(METADATA$FILENAME, 'partition_hour=([0-9]+)', 1, 1, 'e', 1),
                REGEXP_SUBSTR(METADATA$FILENAME, 'provider=([^/]+)', 1, 1, 'e', 1),
                $1:"average_precision"::FLOAT,
                $1:"average_satellites"::FLOAT,
                METADATA$FILENAME
            FROM @mobile_coverage.staging.silver_batch_stage/gps_precision_by_provider/date={{ ds }}/
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\\\.parquet'
        ON_ERROR = 'ABORT_STATEMENT';
        """,
        """
        COPY INTO mobile_coverage.staging.raw_status_count
            (process_date, partition_hour, postal_code, status, status_count, _file_name)
        FROM (
            SELECT
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e', 1)),
                REGEXP_SUBSTR(METADATA$FILENAME, 'partition_hour=([0-9]+)', 1, 1, 'e', 1),
                REGEXP_SUBSTR(METADATA$FILENAME, 'postal_code=([^/]+)', 1, 1, 'e', 1),
                $1:"status"::STRING,
                $1:"status_count"::INTEGER,
                METADATA$FILENAME
            FROM @mobile_coverage.staging.silver_batch_stage/status_count/date={{ ds }}/
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\\\.parquet'
        ON_ERROR = 'ABORT_STATEMENT';
        """,
    ],
    doc_md="""
    ### load_to_snowflake
    Loads yesterday's `silver_batch/` Parquet (all three aggregates) into
    `mobile_coverage.staging` via COPY INTO, scoped to `{{ ds }}` specifically.
    Reconstructs Hive partition columns (date, partition_hour, postal_code/provider)
    from METADATA$FILENAME since Spark strips them from the actual file content.
    """,
)

    # -------------------------------------------------------------------------
    # 4 & 5. dbt — build and test the analytical models
    # -------------------------------------------------------------------------
    # astronomer-cosmos reads the dbt project's dependency graph and expands
    # it into one Airflow task per model: run, then test, wired with the
    # same ref()/source() edges dbt resolves. A failing model shows up as
    # that exact task failing, and independent models run in parallel.
    dbt_tasks = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="mobile_coverage",
            target_name="dev",  # target defined in profiles.yml
            profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
        ),
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
    )

    # -------------------------------------------------------------------------
    # 6. Reconcile streaming vs batch aggregates
    # -------------------------------------------------------------------------
    # This is the core value-add of the whole pipeline. Compare yesterday's
    # streaming aggregates (from Athena over S3) against yesterday's batch
    # aggregates (from Snowflake), compute divergence percentages, write
    # the reconciliation report.
    reconcile = EmptyOperator(
        task_id="reconcile",
        doc_md="""
        ### reconcile
        Compares streaming pipeline outputs (Athena, 2-minute windows
        rolled up to daily) against batch pipeline outputs (Snowflake,
        aggregated daily). Computes per-metric divergence percentages
        and writes the report to `reconciliation.divergence_daily`.

        A row is considered divergent if the absolute percentage
        difference exceeds 1% (configurable per metric).
        """,
    )

    # -------------------------------------------------------------------------
    # 7. Refresh the Streamlit dashboard's cache
    # -------------------------------------------------------------------------
    # The dashboard caches Athena/Snowflake results for 1 hour. After a
    # fresh batch run there's new gold data — invalidate the cache so
    # the next viewer sees it immediately.
    refresh_dashboard_cache = EmptyOperator(
        task_id="refresh_dashboard_cache",
        doc_md="""
        ### refresh_dashboard_cache
        Signals the Streamlit dashboard to invalidate its result cache
        so the next page load queries fresh data.

        **Real implementation:** HttpOperator hitting a
        `/admin/clear-cache` endpoint, or writing a sentinel file to S3
        that the dashboard checks on each request.
        """,
    )

    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
    # Airflow's >> operator expresses "runs after". Read the chain like a
    # data flow diagram.
    #
    # dbt must finish building and testing every model before reconcile
    # runs, so the comparison always reads fresh, validated data. reconcile
    # must complete before the dashboard cache refresh, so the cache is
    # only invalidated once there's new data behind it.
    (
        wait_for_bronze
        >> spark_batch_aggregate
        >> load_to_snowflake
        >> dbt_tasks
        >> reconcile
        >> refresh_dashboard_cache
    )