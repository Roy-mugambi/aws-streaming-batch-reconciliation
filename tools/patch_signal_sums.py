"""Add sum/count columns to the batch side so averages can be weighted.

Usage (from the repo root):
    python3 tools/patch_signal_sums.py
Patches spark/batch_aggregate.py and airflow/dags/mobile_coverage_daily_batch.py.
Each replacement must match exactly once; otherwise nothing is written.
"""
import sys
from pathlib import Path

SPARK = Path("spark/batch_aggregate.py")
DAG = Path("airflow/dags/mobile_coverage_daily_batch.py")

spark_edits = [
    (
        "from pyspark.sql.functions import col, avg, count,",
        "from pyspark.sql.functions import sum as spark_sum\nfrom pyspark.sql.functions import col, avg, count,",
    ),
    (
        '    .agg(avg("signal").alias("average_signal"))\n',
        '    .agg(\n'
        '        avg("signal").alias("average_signal"),\n'
        '        spark_sum("signal").alias("sum_signal"),      # sums + counts let any\n'
        '        count("signal").alias("count_signal"),        # rollup be exactly weighted\n'
        '    )\n',
    ),
    (
        '        avg("satellites").alias("average_satellites"),\n    )\n',
        '        avg("satellites").alias("average_satellites"),\n'
        '        spark_sum("precision").alias("sum_precision"),\n'
        '        count("precision").alias("count_precision"),\n'
        '        spark_sum("satellites").alias("sum_satellites"),\n'
        '        count("satellites").alias("count_satellites"),\n'
        '    )\n',
    ),
]

dag_edits = [
    (
        "(process_date, partition_hour, postal_code, operator, average_signal, _file_name)",
        "(process_date, partition_hour, postal_code, operator, average_signal, sum_signal, count_signal, _file_name)",
    ),
    (
        '$1:"average_signal"::FLOAT,\n',
        '$1:"average_signal"::FLOAT,\n'
        '                $1:"sum_signal"::NUMBER,\n'
        '                $1:"count_signal"::NUMBER,\n',
    ),
    (
        "(process_date, partition_hour, provider, average_precision, average_satellites, _file_name)",
        "(process_date, partition_hour, provider, average_precision, average_satellites, "
        "sum_precision, count_precision, sum_satellites, count_satellites, _file_name)",
    ),
    (
        '$1:"average_satellites"::FLOAT,\n',
        '$1:"average_satellites"::FLOAT,\n'
        '                $1:"sum_precision"::FLOAT,\n'
        '                $1:"count_precision"::NUMBER,\n'
        '                $1:"sum_satellites"::FLOAT,\n'
        '                $1:"count_satellites"::NUMBER,\n',
    ),
]


def patch(path, edits):
    text = path.read_text()
    for old, new in edits:
        n = text.count(old)
        if n != 1:
            sys.exit(f"{path}: expected 1 match, found {n} for:\n{old}\nNothing written.")
        text = text.replace(old, new)
    return text


new_spark = patch(SPARK, spark_edits)
new_dag = patch(DAG, dag_edits)
SPARK.write_text(new_spark)
DAG.write_text(new_dag)
print("patched", SPARK, "and", DAG)
