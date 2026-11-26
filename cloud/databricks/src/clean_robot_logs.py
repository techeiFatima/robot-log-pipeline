"""Databricks transformation: raw robot logs -> clean table, with the success test.

The same cleaning rules as dbt's stg_robot_logs, in PySpark, so the clean
table can be built on Databricks (Delta) from the staged CSVs in S3.

NOT RUN on Databricks (needs a workspace). The same code IS run locally on
Apache Spark (`--format parquet`) in tests and against the real file.

    spark-submit clean_robot_logs.py --input s3://<bucket>/staged/robot_logs/ \\
        --output robot_logs.analytics.stg_robot_logs_spark
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

SENSORS = [f"Current_J{i}" for i in range(6)] + [
    "Temperature_T0",
    *[f"Temperature_J{i}" for i in range(1, 6)],
    *[f"Speed_J{i}" for i in range(6)],
    "Tool_current",
]

RAW_SCHEMA = T.StructType(
    [
        T.StructField("Num", T.LongType()),
        T.StructField("Timestamp", T.StringType()),
        *[
            T.StructField(c, T.DoubleType())
            for c in [
                "Current_J0",
                "Temperature_T0",
                "Current_J1",
                "Temperature_J1",
                "Current_J2",
                "Temperature_J2",
                "Current_J3",
                "Temperature_J3",
                "Current_J4",
                "Temperature_J4",
                "Current_J5",
                "Temperature_J5",
                "Speed_J0",
                "Speed_J1",
                "Speed_J2",
                "Speed_J3",
                "Speed_J4",
                "Speed_J5",
                "Tool_current",
            ]
        ],
        T.StructField("cycle ", T.LongType()),
        T.StructField("Robot_ProtectiveStop", T.DoubleType()),
        T.StructField("grip_lost", T.BooleanType()),
        T.StructField("source_file", T.StringType()),
    ]
)


def read_staged_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the staged CSVs written by robot_pipeline.cloud.s3 (values unchanged)."""
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')  # pandas writes a quote inside a value as ""
        .option("enforceSchema", False)  # check header names against the schema
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def clean(raw: DataFrame) -> DataFrame:
    """Row-for-row cleanup, matching stg_robot_logs (findings 1, 2, 4-8)."""
    ts_text = F.regexp_replace(F.expr("trim(BOTH '\"' FROM `Timestamp`)"), "Z$", "")
    stop = F.col("Robot_ProtectiveStop")
    renamed = [
        F.col(c).alias(c.lower().replace("temperature_t0", "temperature_j0")) for c in SENSORS
    ]
    return raw.select(
        F.concat_ws(":", "source_file", F.col("Num").cast("string")).alias("log_id"),
        "source_file",
        F.col("Num").alias("row_num"),
        F.col("Timestamp").alias("timestamp_raw"),
        F.col("Timestamp").startswith('"').alias("had_stray_quotes"),
        F.to_timestamp_ntz(ts_text, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")).alias("event_ts"),
        *renamed,
        F.col("cycle ").alias("cycle_id"),
        F.when(stop == 1, "stop")
        .when(stop == 0, "no_stop")
        .when(stop.isNull(), "unknown")
        .otherwise("invalid")
        .alias("stop_status"),
        F.when(stop == 1, True).when(stop == 0, False).alias("protective_stop"),
        stop.isNull().alias("is_status_unknown"),
        "grip_lost",
    )


def success_check(raw: DataFrame, cleaned: DataFrame) -> dict:
    """The success test, Spark version: counts come from the data on every run."""
    r = raw.select(
        F.count("*").alias("raw_rows"),
        F.sum(F.when(F.col("Robot_ProtectiveStop") == 1, 1).otherwise(0)).alias("raw_stop_rows"),
        F.sum(F.when(F.col("Robot_ProtectiveStop").isNull(), 1).otherwise(0)).alias(
            "raw_unknown_rows"
        ),
    ).first()
    c = cleaned.select(
        F.count("*").alias("clean_rows"),
        F.sum(F.when(F.col("stop_status") == "stop", 1).otherwise(0)).alias("clean_stop_rows"),
        F.sum(F.when(F.col("stop_status") == "unknown", 1).otherwise(0)).alias(
            "clean_unknown_rows"
        ),
        F.sum(F.when(F.col("stop_status") == "invalid", 1).otherwise(0)).alias("invalid_rows"),
        F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("unparsed_timestamps"),
    ).first()
    joined = raw.alias("r").join(
        cleaned.alias("c"),
        (F.col("r.source_file") == F.col("c.source_file")) & (F.col("r.Num") == F.col("c.row_num")),
        "full_outer",
    )
    missing = joined.filter(
        (
            (F.col("r.Robot_ProtectiveStop") == 1)
            & ~F.coalesce(F.col("c.stop_status") == "stop", F.lit(False))
        )
        | (
            (F.col("c.stop_status") == "stop")
            & ~F.coalesce(F.col("r.Robot_ProtectiveStop") == 1, F.lit(False))
        )
    ).count()
    counts = {**r.asDict(), **c.asDict(), "stop_mismatches": missing}
    counts = {k: int(v or 0) for k, v in counts.items()}
    counts["passed"] = (
        counts["raw_rows"] == counts["clean_rows"]
        and counts["raw_stop_rows"] == counts["clean_stop_rows"]
        and counts["raw_unknown_rows"] == counts["clean_unknown_rows"]
        and counts["invalid_rows"] == 0
        and counts["unparsed_timestamps"] == 0
        and counts["stop_mismatches"] == 0
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="folder of staged CSVs (S3, Volume or local)")
    p.add_argument("--output", required=True, help="table name (delta) or folder (parquet)")
    p.add_argument("--format", default="delta", choices=["delta", "parquet"])
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("clean_robot_logs").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw = read_staged_csv(spark, args.input)
    cleaned = clean(raw)
    result = success_check(raw, cleaned)
    print(f"Success test: {result}")
    if not result["passed"]:
        print("FAILED: not writing the clean table.", file=sys.stderr)
        return 1
    writer = cleaned.write.mode("overwrite")
    if args.format == "delta":
        writer.format("delta").option("overwriteSchema", "true").saveAsTable(args.output)
    else:
        writer.parquet(args.output)
    print(f"Wrote {result['clean_rows']} rows to {args.output} ({args.format})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
