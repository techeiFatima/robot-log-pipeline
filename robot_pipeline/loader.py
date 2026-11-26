"""Load raw robot log Excel files into the DuckDB table ``raw_logs``.

Guarantees:

* The source values are stored unchanged. Only two columns are added:
  ``source_file`` (the file's name) and ``loaded_at`` (when it was loaded, UTC).
* Running the loader twice on the same file never duplicates rows: the rows
  from that file are replaced inside one transaction.
* A missing file, an empty file, or a file whose columns changed fails with a
  message that says exactly what is wrong.
* Every load is recorded in ``load_audit`` (file, checksum, row count, time),
  which the monitoring checks read to spot data that stopped arriving.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

RAW_TABLE = "raw_logs"
AUDIT_TABLE = "load_audit"

# The 24 source columns exactly as the UCI file spells them (including the
# trailing space in "cycle "), with the DuckDB type the raw table stores.
# Types are fixed here rather than guessed per file, so a later file cannot
# silently change a column's type.
EXPECTED_COLUMNS: dict[str, str] = {
    "Num": "BIGINT",
    "Timestamp": "VARCHAR",
    "Current_J0": "DOUBLE",
    "Temperature_T0": "DOUBLE",
    "Current_J1": "DOUBLE",
    "Temperature_J1": "DOUBLE",
    "Current_J2": "DOUBLE",
    "Temperature_J2": "DOUBLE",
    "Current_J3": "DOUBLE",
    "Temperature_J3": "DOUBLE",
    "Current_J4": "DOUBLE",
    "Temperature_J4": "DOUBLE",
    "Current_J5": "DOUBLE",
    "Temperature_J5": "DOUBLE",
    "Speed_J0": "DOUBLE",
    "Speed_J1": "DOUBLE",
    "Speed_J2": "DOUBLE",
    "Speed_J3": "DOUBLE",
    "Speed_J4": "DOUBLE",
    "Speed_J5": "DOUBLE",
    "Tool_current": "DOUBLE",
    "cycle ": "BIGINT",
    "Robot_ProtectiveStop": "DOUBLE",
    "grip_lost": "BOOLEAN",
}
METADATA_COLUMNS: dict[str, str] = {
    "source_file": "VARCHAR",
    "loaded_at": "TIMESTAMPTZ",
}


class LoaderError(Exception):
    """Raised with a human-readable message when a load cannot go ahead."""


@dataclass(frozen=True)
class LoadResult:
    source_file: str
    rows_loaded: int
    rows_replaced: int
    sha256: str
    loaded_at: datetime


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def validate_columns(columns: list[str]) -> None:
    """Fail if any expected column is missing or any unexpected one appears."""
    actual = list(columns)
    expected = list(EXPECTED_COLUMNS)
    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if c not in expected]
    duplicates = sorted({c for c in actual if actual.count(c) > 1})
    problems = []
    if missing:
        problems.append(f"missing columns: {missing!r}")
    if unexpected:
        problems.append(f"unexpected columns: {unexpected!r}")
    if duplicates:
        problems.append(f"duplicate columns: {duplicates!r}")
    if problems:
        raise LoaderError(
            "The source file's columns changed; refusing to load. "
            + "; ".join(problems)
            + ". Names are compared exactly, including spaces and case."
        )


def read_source(path: Path) -> pd.DataFrame:
    """Read one Excel file and validate it, without changing any value."""
    if not path.exists():
        raise LoaderError(
            f"Source file not found: {path}. Download the UR3 CobotOps dataset from "
            "https://archive.ics.uci.edu/dataset/963/ur3+cobotops into data/raw/."
        )
    if not path.is_file():
        raise LoaderError(f"Source path is not a file: {path}")
    try:
        df = pd.read_excel(path)
    except Exception as exc:  # openpyxl raises several unrelated types
        raise LoaderError(f"Could not read {path} as an Excel file: {exc}") from exc
    # pandas turns a duplicated header "x" into "x.1"; read the header row as-is
    # to catch duplicates before that renaming hides them.
    header = list(pd.read_excel(path, header=None, nrows=1).iloc[0].astype(str))
    validate_columns(header)
    if df.empty:
        raise LoaderError(f"Source file has a header but no rows: {path}")
    return df


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    existing = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [RAW_TABLE],
        ).fetchall()
    }
    if existing and not set(METADATA_COLUMNS) <= existing:
        # A raw_logs table made by the first version of ingest.py (no
        # source_file / loaded_at). It is a pure copy of the source file, so it
        # is safe to rebuild from the file.
        log.warning("Rebuilding legacy %s table that lacks load metadata columns", RAW_TABLE)
        con.execute(f"DROP TABLE {RAW_TABLE}")
    columns = {**EXPECTED_COLUMNS, **METADATA_COLUMNS}
    ddl = ",\n  ".join(f"{_quote(name)} {dtype}" for name, dtype in columns.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {RAW_TABLE} (\n  {ddl}\n)")
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
              source_file VARCHAR,
              sha256 VARCHAR,
              rows_loaded BIGINT,
              rows_replaced BIGINT,
              loaded_at TIMESTAMPTZ
            )"""
    )


def load_file(path: Path, db_path: Path) -> LoadResult:
    """Load one file into ``raw_logs``, replacing any earlier load of the same file."""
    path = Path(path)
    df = read_source(path)
    source_file = path.name
    sha = _sha256(path)
    loaded_at = datetime.now(UTC)

    con = duckdb.connect(str(db_path))
    try:
        _ensure_tables(con)
        con.register("incoming", df)
        select_cols = ", ".join(
            f"CAST({_quote(c)} AS {t}) AS {_quote(c)}" for c, t in EXPECTED_COLUMNS.items()
        )
        con.execute("BEGIN TRANSACTION")
        try:
            (replaced,) = con.execute(
                f"SELECT count(*) FROM {RAW_TABLE} WHERE source_file = ?", [source_file]
            ).fetchone()
            con.execute(f"DELETE FROM {RAW_TABLE} WHERE source_file = ?", [source_file])
            con.execute(
                f"INSERT INTO {RAW_TABLE} BY NAME "
                f"SELECT {select_cols}, ? AS source_file, ? AS loaded_at FROM incoming",
                [source_file, loaded_at],
            )
            (loaded,) = con.execute(
                f"SELECT count(*) FROM {RAW_TABLE} WHERE source_file = ?", [source_file]
            ).fetchone()
            con.execute(
                f"INSERT INTO {AUDIT_TABLE} VALUES (?, ?, ?, ?, ?)",
                [source_file, sha, loaded, replaced, loaded_at],
            )
            con.execute("COMMIT")
        except Exception as exc:
            con.execute("ROLLBACK")
            raise LoaderError(f"Load of {source_file} failed and was rolled back: {exc}") from exc
    finally:
        con.close()

    if loaded != len(df):
        raise LoaderError(f"Row count mismatch for {source_file}: read {len(df)}, stored {loaded}")
    return LoadResult(source_file, loaded, replaced, sha, loaded_at)


def find_source_files(raw_dir: Path) -> list[Path]:
    """All Excel files in the raw folder, sorted so loads are repeatable."""
    if not raw_dir.is_dir():
        raise LoaderError(
            f"Raw data folder not found: {raw_dir}. Create it and put the UCI file "
            "dataset_02052023.xlsx in it."
        )
    files = sorted(p for p in raw_dir.glob("*.xlsx") if not p.name.startswith("~$"))
    if not files:
        raise LoaderError(
            f"No .xlsx files in {raw_dir}. Download the UR3 CobotOps dataset from "
            "https://archive.ics.uci.edu/dataset/963/ur3+cobotops."
        )
    return files
