import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import col, avg, count, to_timestamp, date_format, concat, lit, explode, split, regexp_replace, from_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("batch_aggregate")

# ---------------------------------------------------------------------------
# Single positional arg: the process date (YYYY-MM-DD), matching Airflow's
# {{ ds }} — i.e. data_interval_start, "yesterday" relative to the DAG run.
# ---------------------------------------------------------------------------
if len(sys.argv) != 2:
    logger.error("Usage: batch_aggregate.py <process_date YYYY-MM-DD>")
    sys.exit(1)

PROCESS_DATE = sys.argv[1]
process_dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
YEAR, MONTH, DAY = process_dt.strftime("%Y"), process_dt.strftime("%m"), process_dt.strftime("%d")

BRONZE_PATH = (
    f"s3://roy-projects/mobile-network-lab/bronze/raw-events/"
    f"year={YEAR}/month={MONTH}/day={DAY}/hour=*/*.gz"
)
SILVER_OUTPUT = "s3://roy-projects/mobile-network-lab/silver_batch/"

logger.info(f"Processing date: {PROCESS_DATE}")
logger.info(f"Reading bronze from: {BRONZE_PATH}")

spark = SparkSession.builder.appName(f"mobile-coverage-batch-aggregate-{PROCESS_DATE}").getOrCreate()

# ---------------------------------------------------------------------------
# Schema — kept field-for-field identical to streaming/glue_streaming.py,
# INCLUDING the "precission" typo. That typo is the real bronze field name;
# "fixing" it here would break parsing against real data. If the schema
# ever changes, both files need updating together — no shared source of
# truth between them yet, which is a fragility worth noting for later.
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("hour", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("signal", StringType(), True),
    StructField("network", StringType(), True),
    StructField("operator", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("satellites", StringType(), True),
    StructField("precission", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("postal_code", StringType(), True),
])

# Firehose concatenates records with no delimiter ({...}{...}{...}), so
# read.json() would parse only the first object per file and silently drop
# the rest. Read each file whole, split on the object boundary, then parse.
# Works for both glued and newline-delimited files.
raw_text = spark.read.text(BRONZE_PATH, wholetext=True)

raw_df = (
    raw_text
    .select(explode(split(regexp_replace(col("value"), r"\}\s*\{", "}\n{"), "\n")).alias("json"))
    .filter(col("json") != "")
    .select(from_json(col("json"), schema).alias("r"))
    .select("r.*")
)

bronze_count = raw_df.count()
logger.info(f"Parsed {bronze_count} bronze records for {PROCESS_DATE}")
if bronze_count == 0:
    raise ValueError(f"No bronze records parsed for {PROCESS_DATE}")
record_count = raw_df.count()
logger.info(f"Read {record_count} raw records for {PROCESS_DATE}")

if record_count == 0:
    logger.error(f"No records found for {PROCESS_DATE} — failing rather than writing empty aggregates")
    spark.stop()
    sys.exit(1)

# ---------------------------------------------------------------------------
# Reconstruct event_timestamp using the PARTITION date, not current_date().
# "hour" field is a time-of-day string like "05:49:30" (per streaming's
# format string) — combined with the real event date this time.
# ---------------------------------------------------------------------------
df_with_timestamp = (
    raw_df
    .withColumn(
        "event_timestamp",
        to_timestamp(concat(lit(PROCESS_DATE), lit(" "), col("hour")), "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn("partition_hour", date_format(col("event_timestamp"), "HH"))
    .withColumn("signal",     col("signal").cast("int"))
    .withColumn("lat",        col("lat").cast("double"))
    .withColumn("long",       col("long").cast("double"))
    .withColumn("speed",      col("speed").cast("double"))
    .withColumn("satellites", col("satellites").cast("double"))
    .withColumn("precission", col("precission").cast("double"))
    .withColumnRenamed("precission", "precision")
)

# Silent-failure guard: to_timestamp() returns NULL on a format mismatch
# rather than raising. Catch that here instead of discovering it later as
# a mysteriously wrong aggregate.
null_ts_count = df_with_timestamp.filter(col("event_timestamp").isNull()).count()
if null_ts_count > 0:
    logger.warning(
        f"{null_ts_count}/{record_count} records failed event_timestamp parsing "
        f"(hour field format mismatch?) — these will fall under partition_hour=null"
    )

logger.info("Schema parsed and timestamp columns derived")

# ---------------------------------------------------------------------------
# Aggregations — same grouping dimensions and metrics as streaming, minus
# the 2-minute window (batch has no micro-batches to window over) and
# minus the watermark (batch has complete data, no late-arrival concern).
# ---------------------------------------------------------------------------
signal_strength_df = (
    df_with_timestamp
    .groupBy("postal_code", "operator", "partition_hour")
    .agg(
        avg("signal").alias("average_signal"),
        spark_sum("signal").alias("sum_signal"),      # sums + counts let any
        count("signal").alias("count_signal"),        # rollup be exactly weighted
    )
)

gps_precision_df = (
    df_with_timestamp
    .groupBy("provider", "partition_hour")
    .agg(
        avg("precision").alias("average_precision"),
        avg("satellites").alias("average_satellites"),
        spark_sum("precision").alias("sum_precision"),
        count("precision").alias("count_precision"),
        spark_sum("satellites").alias("sum_satellites"),
        count("satellites").alias("count_satellites"),
    )
)

status_count_df = (
    df_with_timestamp
    .groupBy("postal_code", "status", "partition_hour")
    .agg(count("*").alias("status_count"))
)

logger.info("Aggregation DataFrames computed")


def write_parquet(df, name: str, partition_cols: list[str]):
    (
        df.write
        .mode("overwrite")  # safe here: scoped to this date's own subfolder, so
        .partitionBy(*partition_cols)  # this is "replace today's output", not "replace everything"
        .parquet(f"{SILVER_OUTPUT}{name}/date={PROCESS_DATE}/")
    )
    logger.info(f"Wrote {name} → {SILVER_OUTPUT}{name}/date={PROCESS_DATE}/")


write_parquet(signal_strength_df, "signal_strength_by_operator", ["partition_hour", "postal_code"])
write_parquet(gps_precision_df, "gps_precision_by_provider", ["partition_hour", "provider"])
write_parquet(status_count_df, "status_count", ["partition_hour", "postal_code"])

logger.info(f"Batch aggregation complete for {PROCESS_DATE}")
spark.stop()