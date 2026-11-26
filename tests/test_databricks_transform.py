"""The Databricks transformation, run locally on Apache Spark with SYNTHETIC data.
Skipped when PySpark is not installed (a separate CI job installs it)."""

import importlib.util
import os
from pathlib import Path

import pytest

if not os.environ.get("REQUIRE_SPARK"):
    pytest.importorskip("pyspark")

SRC = Path(__file__).resolve().parents[1] / "cloud" / "databricks" / "src" / "clean_robot_logs.py"


@pytest.fixture(scope="module")
def job():
    spec = importlib.util.spec_from_file_location("clean_robot_logs", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def staged(tmp_path, df, name="SYNTHETIC.xlsx"):
    from robot_pipeline.cloud.s3 import to_staged_csv

    folder = tmp_path / "staged"
    folder.mkdir()
    (folder / "part.csv").write_bytes(to_staged_csv(df, name))
    return str(folder)


def test_clean_matches_the_rules_and_passes_the_success_test(job, spark, tmp_path):
    from robot_pipeline.synthetic import make_frame

    df = make_frame(rows=200, seed=3)
    raw = job.read_staged_csv(spark, staged(tmp_path, df))
    cleaned = job.clean(raw)
    result = job.success_check(raw, cleaned)

    assert result["passed"], result
    assert result["raw_stop_rows"] == int((df["Robot_ProtectiveStop"] == 1).sum())
    assert result["raw_unknown_rows"] == int(df["Robot_ProtectiveStop"].isna().sum())
    rows = {r["row_num"]: r for r in cleaned.collect()}
    quoted = df.index[df["Timestamp"].str.startswith('"')][0]
    r = rows[int(df.loc[quoted, "Num"])]
    assert r["had_stray_quotes"] and r["event_ts"] is not None
    assert "temperature_j0" in cleaned.columns and "cycle_id" in cleaned.columns


def test_unexpected_stop_value_fails_the_success_test(job, spark, tmp_path):
    from robot_pipeline.synthetic import make_frame

    df = make_frame(rows=50, seed=4)
    df.loc[3, "Robot_ProtectiveStop"] = 2.0
    raw = job.read_staged_csv(spark, staged(tmp_path, df))
    result = job.success_check(raw, job.clean(raw))
    assert not result["passed"] and result["invalid_rows"] == 1
