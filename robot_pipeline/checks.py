"""The success test: compare the clean table against the raw table on every run.

It passes only if:

* every raw row marked as a stop (Robot_ProtectiveStop = 1) is a 'stop' row in
  stg_robot_logs, matched row by row on (source_file, Num), and the clean table
  has no stop that raw does not have;
* every raw row with an unknown status (NULL) is an 'unknown' row in the clean
  table, matched row by row, and is counted and reported separately;
* the clean table has exactly as many rows as raw (nothing dropped or added);
* no row has an unexpected stop value ('invalid').

All counts are queried from the database each time. Nothing is hardcoded.

Usage:
    python -m robot_pipeline.checks            # exit 1 if any check fails
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from robot_pipeline import config

CLEAN_TABLE = "stg_robot_logs"

COUNTS_SQL = f"""
with raw as (
    select source_file, "Num" as row_num, "Robot_ProtectiveStop" as stop from raw_logs
),
clean as (
    select source_file, row_num, stop_status from {CLEAN_TABLE}
),
joined as (
    select raw.stop, clean.stop_status, raw.row_num as raw_num, clean.row_num as clean_num
    from raw
    full outer join clean
      on raw.source_file = clean.source_file and raw.row_num = clean.row_num
)
select
    (select count(*) from raw)                                       as raw_rows,
    (select count(*) from clean)                                     as clean_rows,
    (select count(*) from raw where stop = 1)                        as raw_stop_rows,
    (select count(*) from clean where stop_status = 'stop')          as clean_stop_rows,
    (select count(*) from raw where stop is null)                    as raw_unknown_rows,
    (select count(*) from clean where stop_status = 'unknown')       as clean_unknown_rows,
    (select count(*) from clean where stop_status = 'invalid')       as clean_invalid_rows,
    count(*) filter (where stop = 1 and coalesce(stop_status, '') <> 'stop')
                                                                     as stops_missing_in_clean,
    count(*) filter (where stop_status = 'stop' and (stop is null or stop <> 1))
                                                                     as stops_not_in_raw,
    count(*) filter (where raw_num is not null and stop is null
                     and coalesce(stop_status, '') <> 'unknown')     as unknown_mismatched,
    count(*) filter (where raw_num is null)                          as rows_only_in_clean,
    count(*) filter (where clean_num is null)                        as rows_only_in_raw
from joined
"""


@dataclass
class SuccessReport:
    checked_at: str
    db_path: str
    counts: dict[str, int]
    failures: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures

    def to_dict(self) -> dict:
        return {**asdict(self), "passed": self.passed}


def evaluate(counts: dict[str, int]) -> list[str]:
    """Turn the raw-versus-clean counts into a list of failure messages."""
    c = counts
    failures = []
    if c["raw_rows"] != c["clean_rows"]:
        failures.append(f"row count differs: raw {c['raw_rows']}, clean {c['clean_rows']}")
    if c["rows_only_in_raw"] or c["rows_only_in_clean"]:
        failures.append(
            f"rows do not match by key: {c['rows_only_in_raw']} only in raw, "
            f"{c['rows_only_in_clean']} only in clean"
        )
    if c["stops_missing_in_clean"]:
        failures.append(f"{c['stops_missing_in_clean']} raw stop rows are not stops in clean")
    if c["stops_not_in_raw"]:
        failures.append(f"{c['stops_not_in_raw']} clean stop rows are not stops in raw")
    if c["raw_stop_rows"] != c["clean_stop_rows"]:
        failures.append(
            f"stop count differs: raw {c['raw_stop_rows']}, clean {c['clean_stop_rows']}"
        )
    if c["raw_unknown_rows"] != c["clean_unknown_rows"] or c["unknown_mismatched"]:
        failures.append(
            f"unknown-status rows differ: raw {c['raw_unknown_rows']}, clean "
            f"{c['clean_unknown_rows']}, mismatched by key {c['unknown_mismatched']}"
        )
    if c["clean_invalid_rows"]:
        failures.append(f"{c['clean_invalid_rows']} rows have an unexpected stop value")
    return failures


def run_success_check(db_path: Path | None = None) -> SuccessReport:
    db_path = Path(db_path or config.db_path())
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}. Run ingest.py and dbt first.")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cursor = con.execute(COUNTS_SQL)
        names = [d[0] for d in cursor.description]
        counts = dict(zip(names, cursor.fetchone(), strict=True))
    finally:
        con.close()
    counts = {k: int(v) for k, v in counts.items()}
    return SuccessReport(
        checked_at=datetime.now(UTC).isoformat(timespec="seconds"),
        db_path=str(db_path),
        counts=counts,
        failures=evaluate(counts),
    )


def write_report(report: SuccessReport, state_dir: Path | None = None) -> Path:
    """Save the report as JSON so monitoring and the docs can read real results."""
    out_dir = Path(state_dir or config.state_dir()) / "checks"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "success_check_latest.json"
    path.write_text(json.dumps(report.to_dict(), indent=2))
    return path


def format_report(report: SuccessReport) -> str:
    c = report.counts
    lines = [
        "Success test: raw_logs vs stg_robot_logs",
        f"  rows              raw {c['raw_rows']:>6}   clean {c['clean_rows']:>6}",
        f"  stop rows         raw {c['raw_stop_rows']:>6}   clean {c['clean_stop_rows']:>6}",
        f"  unknown status    raw {c['raw_unknown_rows']:>6}   clean {c['clean_unknown_rows']:>6}"
        "   (reported separately, never dropped or guessed)",
        f"  stops missing in clean {c['stops_missing_in_clean']}, "
        f"stops not in raw {c['stops_not_in_raw']}, invalid values {c['clean_invalid_rows']}",
        "  RESULT: " + ("PASS" if report.passed else "FAIL"),
    ]
    lines += [f"  - {msg}" for msg in report.failures]
    return "\n".join(lines)


def main() -> int:
    report = run_success_check()
    path = write_report(report)
    print(format_report(report))
    print(f"  report: {path}")
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
