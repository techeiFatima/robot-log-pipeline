"""Load raw robot log Excel files into the DuckDB table ``raw_logs``.

Guarantees:

* The source values are stored unchanged. Only two columns are added:
  ``source_file`` (the file's name) and ``loaded_at`` (when it was loaded, UTC).
* Running the loader twice on the same file never duplicates rows. An
  unchanged file (same SHA-256) is left alone; a changed file's rows are
  replaced. All files of one run load in a single transaction.
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
    action: str  # "loaded", "replaced", "reloaded" (same content rewritten) or "unchanged"


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


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    """First row of a query, or None. Fetches all rows so no result stays open
    (DuckDB 2 refuses new statements while a result is only partly read)."""
    rows = con.execute(sql, params or []).fetchall()
    return rows[0] if rows else None


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create or upgrade the tables. Called inside the load transaction, so a
    failed load also undoes any rebuild done here."""
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
        # is safe to rebuild from the file; the transaction restores it on failure.
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
              loaded_at TIMESTAMPTZ,
              action VARCHAR
            )"""
    )
    # Audit tables made before the "action" column existed.
    con.execute(f"ALTER TABLE {AUDIT_TABLE} ADD COLUMN IF NOT EXISTS action VARCHAR")


def _last_sha(con: duckdb.DuckDBPyConnection, source_file: str) -> str | None:
    """Checksum of the last load that actually wrote this file's rows."""
    row = _one(
        con,
        f"""SELECT sha256 FROM {AUDIT_TABLE}
            WHERE source_file = ? AND coalesce(action, 'loaded') <> 'unchanged'
            ORDER BY loaded_at DESC LIMIT 1""",
        [source_file],
    )
    return row[0] if row else None


def _is_unchanged(con: duckdb.DuckDBPyConnection, source_file: str, sha: str) -> bool:
    """True if this exact content is already fully loaded under this name."""
    last = _one(
        con,
        f"""SELECT sha256, rows_loaded FROM {AUDIT_TABLE}
            WHERE source_file = ? AND coalesce(action, 'loaded') <> 'unchanged'
            ORDER BY loaded_at DESC LIMIT 1""",
        [source_file],
    )
    if not last or last[0] != sha:
        return False
    (present,) = _one(con, f"SELECT count(*) FROM {RAW_TABLE} WHERE source_file = ?", [source_file])
    return present == last[1]


def load_files(paths: list[Path], db_path: Path, force: bool = False) -> list[LoadResult]:
    """Load several files as ONE transaction: either all succeed or nothing changes.

    * Every file is read and validated before the database is touched.
    * A file whose content (SHA-256) is already loaded under the same name is
      left alone (action ``unchanged``), so ``loaded_at`` keeps meaning "when
      this data arrived". ``force=True`` reloads it anyway.
    * Otherwise the file's earlier rows are replaced (``loaded`` / ``replaced``).
    """
    sources = []
    for path in paths:
        path = Path(path)
        sources.append((path, read_source(path), _sha256(path)))
    names = [p.name for p, _, _ in sources]
    if len(set(names)) != len(names):
        raise LoaderError(f"Two files share a name; source_file must be unique: {names}")

    loaded_at = datetime.now(UTC)
    results: list[LoadResult] = []
    select_cols = ", ".join(
        f"CAST({_quote(c)} AS {t}) AS {_quote(c)}" for c, t in EXPECTED_COLUMNS.items()
    )
    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            _ensure_tables(con)
            for path, df, sha in sources:
                name = path.name
                (before,) = _one(
                    con, f"SELECT count(*) FROM {RAW_TABLE} WHERE source_file = ?", [name]
                )
                same_content = _last_sha(con, name) == sha
                if not force and _is_unchanged(con, name, sha):
                    action, replaced, stored = "unchanged", 0, before
                else:
                    con.register("incoming", df)
                    con.execute(f"DELETE FROM {RAW_TABLE} WHERE source_file = ?", [name])
                    con.execute(
                        f"INSERT INTO {RAW_TABLE} BY NAME "
                        f"SELECT {select_cols}, ? AS source_file, ? AS loaded_at FROM incoming",
                        [name, loaded_at],
                    )
                    con.unregister("incoming")
                    (stored,) = _one(
                        con, f"SELECT count(*) FROM {RAW_TABLE} WHERE source_file = ?", [name]
                    )
                    if stored != len(df):
                        raise LoaderError(f"Row count mismatch: read {len(df)}, stored {stored}")
                    if not before:
                        action = "loaded"
                    elif same_content:
                        action = "reloaded"  # forced or repaired; same content, not new data
                    else:
                        action = "replaced"
                    replaced = before
                con.execute(
                    f"INSERT INTO {AUDIT_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                    [name, sha, stored, replaced, loaded_at, action],
                )
                results.append(LoadResult(name, stored, replaced, sha, loaded_at, action))
            con.execute("COMMIT")
        except Exception as exc:
            con.execute("ROLLBACK")
            raise LoaderError(f"Load failed and was rolled back, nothing changed: {exc}") from exc
    finally:
        con.close()
    return results


def load_file(path: Path, db_path: Path, force: bool = False) -> LoadResult:
    """Load one file into ``raw_logs`` (see :func:`load_files`)."""
    return load_files([path], db_path, force)[0]


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
