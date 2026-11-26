"""End-to-end: loader -> dbt build -> success test, on a SYNTHETIC file.

Slower (it runs dbt), so it is marked "integration". Run only these with
`pytest -m integration`, or skip them with `pytest -m "not integration"`.
"""

import duckdb
import pytest

from robot_pipeline.checks import run_success_check
from robot_pipeline.dbt_runner import run_dbt
from robot_pipeline.loader import load_file
from robot_pipeline.synthetic import make_frame, write_sample

pytestmark = pytest.mark.integration


def dbt_build(db_path, tmp_path):
    return run_dbt(
        ["build", "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs")],
        db_path=db_path,
    )


def test_full_pipeline_passes_on_synthetic_sample(tmp_path, db_path):
    sample = write_sample(tmp_path / "SYNTHETIC_sample.xlsx", rows=300, seed=42)
    load_file(sample, db_path)

    assert dbt_build(db_path, tmp_path) == 0

    report = run_success_check(db_path)
    assert report.passed, report.failures
    con = duckdb.connect(str(db_path), read_only=True)
    (events,) = con.execute("SELECT count(*) FROM fct_stop_events").fetchone()
    con.close()
    assert events > 0


def test_unexpected_stop_value_fails_the_dbt_build(tmp_path, db_path):
    df = make_frame(rows=60, seed=5)
    df.loc[10, "Robot_ProtectiveStop"] = 2.0  # neither 0, 1 nor missing
    path = tmp_path / "SYNTHETIC_bad_value.xlsx"
    df.to_excel(path, index=False)
    load_file(path, db_path)

    assert dbt_build(db_path, tmp_path) != 0
    assert not run_success_check(db_path).passed


def test_report_and_data_dictionary_run_on_a_built_database(tmp_path, db_path):
    from robot_pipeline import data_dictionary, report

    sample = write_sample(tmp_path / "SYNTHETIC_sample.xlsx", rows=300, seed=42)
    load_file(sample, db_path)
    assert dbt_build(db_path, tmp_path) == 0

    numbers = report.collect(db_path)
    assert numbers["raw_rows"] == 300
    assert numbers["stop_rows"] + numbers["no_stop_rows"] + numbers["unknown_status_rows"] == 300
    assert numbers["by_hour"] and numbers["conditions"]

    md = data_dictionary.build(db_path, tmp_path / "target" / "manifest.json")
    for table in ["raw_logs", "load_audit", "stg_robot_logs", "fct_stop_events", "stream_events"]:
        assert f"`{table}`" in md
    assert "|  |" not in md  # every column has a description
