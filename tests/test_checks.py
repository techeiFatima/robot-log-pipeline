"""Tests for the success test (robot_pipeline.checks).

These build a raw table with the loader and then write a *fake* clean table by
hand, so each test can break the clean data in one specific way and prove the
check notices. SYNTHETIC data only.
"""

import json

import duckdb
import pytest

from robot_pipeline.checks import run_success_check, write_report
from robot_pipeline.loader import load_file

FAKE_CLEAN = """
CREATE OR REPLACE TABLE stg_robot_logs AS
SELECT source_file, "Num" AS row_num,
       CASE WHEN "Robot_ProtectiveStop" = 1 THEN 'stop'
            WHEN "Robot_ProtectiveStop" = 0 THEN 'no_stop'
            ELSE 'unknown' END AS stop_status
FROM raw_logs
"""


@pytest.fixture
def loaded_db(sample_file, db_path):
    load_file(sample_file, db_path)
    con = duckdb.connect(str(db_path))
    con.execute(FAKE_CLEAN)
    con.close()
    return db_path


def mutate(db, sql):
    con = duckdb.connect(str(db))
    con.execute(sql)
    con.close()


def raw_count(db, where):
    con = duckdb.connect(str(db), read_only=True)
    (n,) = con.execute(f"SELECT count(*) FROM raw_logs WHERE {where}").fetchone()
    con.close()
    return n


def test_correct_clean_table_passes_and_counts_come_from_raw(loaded_db):
    report = run_success_check(loaded_db)

    assert report.passed, report.failures
    c = report.counts
    assert c["raw_stop_rows"] == raw_count(loaded_db, "Robot_ProtectiveStop = 1") > 0
    assert c["raw_unknown_rows"] == raw_count(loaded_db, "Robot_ProtectiveStop IS NULL") > 0
    assert c["clean_stop_rows"] == c["raw_stop_rows"]
    assert c["clean_unknown_rows"] == c["raw_unknown_rows"]


def test_a_lost_stop_fails(loaded_db):
    mutate(
        loaded_db,
        "UPDATE stg_robot_logs SET stop_status = 'no_stop' "
        "WHERE row_num = (SELECT min(row_num) FROM stg_robot_logs WHERE stop_status = 'stop')",
    )
    report = run_success_check(loaded_db)
    assert not report.passed
    assert report.counts["stops_missing_in_clean"] == 1


def test_an_invented_stop_fails(loaded_db):
    mutate(
        loaded_db,
        "UPDATE stg_robot_logs SET stop_status = 'stop' "
        "WHERE row_num = (SELECT min(row_num) FROM stg_robot_logs WHERE stop_status = 'no_stop')",
    )
    report = run_success_check(loaded_db)
    assert not report.passed
    assert report.counts["stops_not_in_raw"] == 1


def test_a_swapped_stop_fails_even_when_totals_match(loaded_db):
    # Same number of stops, but on the wrong row: totals alone would miss this.
    mutate(
        loaded_db,
        """UPDATE stg_robot_logs SET stop_status = CASE stop_status
                WHEN 'stop' THEN 'no_stop' ELSE 'stop' END
           WHERE row_num IN (
             (SELECT min(row_num) FROM stg_robot_logs WHERE stop_status = 'stop'),
             (SELECT min(row_num) FROM stg_robot_logs WHERE stop_status = 'no_stop'))""",
    )
    report = run_success_check(loaded_db)
    assert report.counts["clean_stop_rows"] == report.counts["raw_stop_rows"]
    assert not report.passed


def test_a_dropped_unknown_row_fails(loaded_db):
    mutate(
        loaded_db,
        "DELETE FROM stg_robot_logs WHERE row_num = "
        "(SELECT min(row_num) FROM stg_robot_logs WHERE stop_status = 'unknown')",
    )
    report = run_success_check(loaded_db)
    assert not report.passed
    assert any("unknown-status rows differ" in f for f in report.failures)
    assert any("row count differs" in f for f in report.failures)


def test_a_guessed_unknown_row_fails(loaded_db):
    mutate(
        loaded_db,
        "UPDATE stg_robot_logs SET stop_status = 'no_stop' WHERE stop_status = 'unknown'",
    )
    report = run_success_check(loaded_db)
    assert not report.passed
    assert report.counts["clean_unknown_rows"] == 0


def test_an_invalid_value_fails(loaded_db):
    mutate(loaded_db, "UPDATE stg_robot_logs SET stop_status = 'invalid' WHERE row_num = 2")
    assert not run_success_check(loaded_db).passed


def test_report_is_written_as_json(loaded_db, tmp_path):
    path = write_report(run_success_check(loaded_db), tmp_path)
    data = json.loads(path.read_text())
    assert data["passed"] is True
    assert set(data["counts"]) >= {"raw_stop_rows", "clean_stop_rows", "raw_unknown_rows"}


def test_missing_database_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError, match="Database not found"):
        run_success_check(tmp_path / "missing.duckdb")
