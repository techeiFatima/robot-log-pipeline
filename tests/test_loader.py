"""Unit tests for robot_pipeline.loader, run on SYNTHETIC files."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from robot_pipeline.loader import (
    EXPECTED_COLUMNS,
    LoaderError,
    find_source_files,
    load_file,
    load_files,
    validate_columns,
)
from robot_pipeline.synthetic import make_frame, write_sample


def query(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_load_stores_every_row_and_adds_metadata(sample_file, db_path):
    result = load_file(sample_file, db_path)

    assert result.rows_loaded == 120
    assert result.rows_replaced == 0
    cols = [r[0] for r in query(db_path, "SELECT column_name FROM (DESCRIBE raw_logs)")]
    assert cols == [*EXPECTED_COLUMNS, "source_file", "loaded_at"]
    assert query(db_path, "SELECT DISTINCT source_file FROM raw_logs") == [(sample_file.name,)]
    assert query(db_path, "SELECT count(*) FROM raw_logs WHERE loaded_at IS NULL") == [(0,)]


def test_raw_values_are_unchanged(sample_file, db_path):
    load_file(sample_file, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    stored = con.execute(
        'SELECT * EXCLUDE (source_file, loaded_at) FROM raw_logs ORDER BY "Num"'
    ).df()
    con.close()
    source = pd.read_excel(sample_file)
    assert_frame_equal(source, stored, check_dtype=False)
    # The quirks survive: stray quotes and NULL stop values are not "fixed" in raw.
    assert query(db_path, """SELECT count(*) FROM raw_logs WHERE "Timestamp" LIKE '"%'""")[0][0]
    assert query(db_path, "SELECT count(*) FROM raw_logs WHERE Robot_ProtectiveStop IS NULL")[0][0]


def test_running_twice_does_not_duplicate_rows(sample_file, db_path):
    first = load_file(sample_file, db_path)
    second = load_file(sample_file, db_path)

    assert first.action == "loaded"
    assert second.action == "unchanged"
    assert second.rows_loaded == first.rows_loaded == 120
    assert query(db_path, "SELECT count(*), count(DISTINCT Num) FROM raw_logs") == [(120, 120)]
    assert query(db_path, "SELECT action FROM load_audit ORDER BY loaded_at") == [
        ("loaded",),
        ("unchanged",),
    ]


def test_unchanged_file_keeps_its_original_loaded_at(sample_file, db_path):
    # So "when did data last arrive?" is not fooled by daily re-runs.
    load_file(sample_file, db_path)
    (before,) = query(db_path, "SELECT max(loaded_at) FROM raw_logs")[0]
    load_file(sample_file, db_path)
    assert query(db_path, "SELECT max(loaded_at) FROM raw_logs")[0][0] == before


def test_force_reloads_an_unchanged_file_without_duplicates(sample_file, db_path):
    load_file(sample_file, db_path)
    result = load_file(sample_file, db_path, force=True)
    assert result.action == "reloaded" and result.rows_replaced == 120
    assert query(db_path, "SELECT count(*) FROM raw_logs") == [(120,)]


def test_deleted_rows_are_restored_even_if_file_is_unchanged(sample_file, db_path):
    load_file(sample_file, db_path)
    con = duckdb.connect(str(db_path))
    con.execute("DELETE FROM raw_logs WHERE Num <= 10")
    con.close()
    assert load_file(sample_file, db_path).action == "reloaded"
    assert query(db_path, "SELECT count(*) FROM raw_logs") == [(120,)]


def test_reloading_a_corrected_file_replaces_its_rows(tmp_path, db_path):
    path = tmp_path / "SYNTHETIC_day.xlsx"
    write_sample(path, rows=50, seed=1)
    load_file(path, db_path)
    write_sample(path, rows=40, seed=2)  # same name, corrected content

    result = load_file(path, db_path)

    assert result.rows_replaced == 50
    assert query(db_path, "SELECT count(*) FROM raw_logs") == [(40,)]


def test_two_files_are_kept_separately(tmp_path, db_path):
    a = write_sample(tmp_path / "SYNTHETIC_a.xlsx", rows=30, seed=1)
    b = write_sample(tmp_path / "SYNTHETIC_b.xlsx", rows=20, seed=2)
    load_file(a, db_path)
    load_file(b, db_path)
    load_file(a, db_path)

    rows = query(db_path, "SELECT source_file, count(*) FROM raw_logs GROUP BY 1 ORDER BY 1")
    assert rows == [("SYNTHETIC_a.xlsx", 30), ("SYNTHETIC_b.xlsx", 20)]


def test_missing_file_fails_with_clear_message(tmp_path, db_path):
    with pytest.raises(LoaderError, match="Source file not found"):
        load_file(tmp_path / "does_not_exist.xlsx", db_path)
    assert not db_path.exists()


def test_missing_folder_and_empty_folder_fail_clearly(tmp_path):
    with pytest.raises(LoaderError, match="folder not found"):
        find_source_files(tmp_path / "nope")
    (tmp_path / "empty").mkdir()
    with pytest.raises(LoaderError, match="No .xlsx files"):
        find_source_files(tmp_path / "empty")


@pytest.mark.parametrize(
    "change, expected",
    [
        (lambda df: df.rename(columns={"cycle ": "cycle"}), "missing columns: \\['cycle '\\]"),
        (lambda df: df.drop(columns=["grip_lost"]), "missing columns: \\['grip_lost'\\]"),
        (lambda df: df.assign(extra=1), "unexpected columns: \\['extra'\\]"),
        (lambda df: df.rename(columns={"Num": "num"}), "missing columns: \\['Num'\\]"),
    ],
    ids=["renamed", "dropped", "added", "case-changed"],
)
def test_changed_columns_fail_and_load_nothing(tmp_path, db_path, change, expected):
    path = tmp_path / "SYNTHETIC_changed.xlsx"
    change(make_frame(rows=10)).to_excel(path, index=False)

    with pytest.raises(LoaderError, match=expected):
        load_file(path, db_path)
    assert not db_path.exists()


def test_duplicate_header_is_rejected():
    cols = list(EXPECTED_COLUMNS) + ["grip_lost"]
    with pytest.raises(LoaderError, match="duplicate columns"):
        validate_columns(cols)


def test_header_only_file_fails(tmp_path, db_path):
    path = tmp_path / "SYNTHETIC_empty.xlsx"
    make_frame(rows=10).iloc[0:0].to_excel(path, index=False)
    with pytest.raises(LoaderError, match="no rows"):
        load_file(path, db_path)


def test_not_an_excel_file_fails(tmp_path, db_path):
    path = tmp_path / "broken.xlsx"
    path.write_text("this is not a spreadsheet")
    with pytest.raises(LoaderError, match="Could not read"):
        load_file(path, db_path)


def test_bad_value_rolls_back_and_keeps_earlier_load(tmp_path, db_path):
    path = tmp_path / "SYNTHETIC_day.xlsx"
    write_sample(path, rows=25, seed=3)
    load_file(path, db_path)
    bad = make_frame(rows=25, seed=3).astype({"Num": "object"})
    bad.loc[5, "Num"] = "not a number"
    bad.to_excel(path, index=False)

    with pytest.raises(LoaderError, match="rolled back"):
        load_file(path, db_path)
    # The earlier good load is untouched: the delete was rolled back too.
    assert query(db_path, "SELECT count(*) FROM raw_logs") == [(25,)]
    assert query(db_path, "SELECT count(*) FROM load_audit") == [(1,)]


def test_a_bad_file_in_a_batch_changes_nothing(tmp_path, db_path):
    good = write_sample(tmp_path / "SYNTHETIC_a.xlsx", rows=30, seed=1)
    load_file(good, db_path)
    write_sample(good, rows=10, seed=9)  # corrected content, would replace 30 rows
    bad_df = make_frame(rows=10, seed=3).astype({"Num": "object"})
    bad_df.loc[2, "Num"] = "oops"
    bad = tmp_path / "SYNTHETIC_b.xlsx"
    bad_df.to_excel(bad, index=False)

    with pytest.raises(LoaderError, match="nothing changed"):
        load_files([good, bad], db_path)
    assert query(db_path, "SELECT source_file, count(*) FROM raw_logs GROUP BY 1") == [
        ("SYNTHETIC_a.xlsx", 30)
    ]


def test_an_invalid_file_is_caught_before_the_database_is_touched(tmp_path, db_path):
    good = write_sample(tmp_path / "SYNTHETIC_a.xlsx", rows=30, seed=1)
    bad = tmp_path / "SYNTHETIC_b.xlsx"
    make_frame(rows=5).drop(columns=["grip_lost"]).to_excel(bad, index=False)
    with pytest.raises(LoaderError, match="columns changed"):
        load_files([good, bad], db_path)
    assert not db_path.exists()


def test_failed_load_keeps_a_legacy_table(tmp_path, db_path):
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE raw_logs AS SELECT 1 AS Num")
    con.close()
    bad_df = make_frame(rows=10, seed=3).astype({"Num": "object"})
    bad_df.loc[2, "Num"] = "oops"
    bad = tmp_path / "SYNTHETIC_bad.xlsx"
    bad_df.to_excel(bad, index=False)

    with pytest.raises(LoaderError):
        load_file(bad, db_path)
    assert query(db_path, "SELECT * FROM raw_logs") == [(1,)]


def test_legacy_table_without_metadata_is_rebuilt(sample_file, db_path):
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE raw_logs AS SELECT 1 AS Num")
    con.close()

    load_file(sample_file, db_path)

    assert query(db_path, "SELECT count(*) FROM raw_logs") == [(120,)]


def test_find_source_files_is_sorted_and_skips_lock_files(tmp_path):
    for name in ["b.xlsx", "a.xlsx", "~$a.xlsx", "notes.txt"]:
        (tmp_path / name).write_text("x")
    assert [p.name for p in find_source_files(tmp_path)] == ["a.xlsx", "b.xlsx"]
