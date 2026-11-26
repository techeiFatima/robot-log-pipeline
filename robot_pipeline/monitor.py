"""Pipeline health checks that raise alerts.

Checks (each one has a runbook in docs/runbooks.md):

* data_stopped_arriving  no new or changed file loaded for too long
* row_count_low          the latest load is far smaller than earlier loads
* hourly_rows_low        an hour has far fewer log rows than a normal hour
* check_failed           the success test, a dbt test, or the pipeline run failed
* stream_silent          (with --stream) no stream event received for too long

Usage:
    python -m robot_pipeline.monitor              # run checks, send alerts, exit 0
    python -m robot_pipeline.monitor --stream     # also watch the Kafka consumer's store
    python -m robot_pipeline.monitor --fail-on-alert   # exit 1 if any alert fired
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from robot_pipeline import config
from robot_pipeline.alerts import Alert, send

log = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass
class Settings:
    max_hours_without_data: float = field(
        default_factory=lambda: _env_float("ROBOT_MAX_HOURS_WITHOUT_DATA", 26)
    )
    low_volume_ratio: float = field(
        default_factory=lambda: _env_float("ROBOT_LOW_VOLUME_RATIO", 0.5)
    )
    min_load_history: int = 3
    max_stream_silence_minutes: float = field(
        default_factory=lambda: _env_float("ROBOT_MAX_STREAM_SILENCE_MINUTES", 5)
    )
    cooldown_seconds: float = 6 * 3600


@dataclass
class CheckResult:
    name: str
    ok: bool
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def _rows(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def _table_exists(con, name: str) -> bool:
    return bool(_rows(con, "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [name]))


def check_data_arrival(con, now: datetime, s: Settings) -> CheckResult:
    """Data counts as arrived only for loads that brought new or changed content."""
    if not _table_exists(con, "load_audit"):
        return CheckResult("data_stopped_arriving", False, "critical", "No load has ever run.")
    ((last,),) = _rows(
        con,
        "SELECT max(loaded_at) FROM load_audit "
        "WHERE coalesce(action, 'loaded') IN ('loaded', 'replaced')",
    )
    if last is None:
        return CheckResult("data_stopped_arriving", False, "critical", "No data was ever loaded.")
    age_h = (now - last).total_seconds() / 3600
    details = {"last_new_data_at": last.isoformat(), "hours_since": round(age_h, 2)}
    if age_h > s.max_hours_without_data:
        return CheckResult(
            "data_stopped_arriving",
            False,
            "critical",
            f"No new data for {age_h:.1f} h (limit {s.max_hours_without_data:g} h). "
            f"Last new data loaded at {last.isoformat()}.",
            details,
        )
    return CheckResult(
        "data_stopped_arriving", True, "info", f"Last new data {age_h:.1f} h ago.", details
    )


def check_load_volume(con, s: Settings) -> CheckResult:
    if not _table_exists(con, "load_audit"):
        return CheckResult("row_count_low", True, "info", "No loads yet.")
    loads = _rows(
        con,
        "SELECT source_file, rows_loaded, loaded_at FROM load_audit "
        "WHERE coalesce(action, 'loaded') IN ('loaded', 'replaced') ORDER BY loaded_at",
    )
    if not loads:
        return CheckResult("row_count_low", True, "info", "No loads yet.")
    latest = loads[-1]
    history = [r[1] for r in loads[:-1]][-10:]
    if len(history) < s.min_load_history:
        return CheckResult(
            "row_count_low",
            True,
            "info",
            f"Only {len(history)} earlier load(s); need {s.min_load_history} to judge normal.",
            {"latest_rows": latest[1]},
        )
    normal = statistics.median(history)
    details = {"latest_file": latest[0], "latest_rows": latest[1], "normal_rows": normal}
    if latest[1] < s.low_volume_ratio * normal:
        return CheckResult(
            "row_count_low",
            False,
            "critical",
            f"Latest load {latest[0]} has {latest[1]} rows; normal is about {normal:g} "
            f"(alert below {s.low_volume_ratio:.0%}).",
            details,
        )
    return CheckResult("row_count_low", True, "info", "Latest load volume is normal.", details)


def check_hourly_volume(con, s: Settings) -> CheckResult:
    """Within the data itself: hours with far fewer rows than the median hour."""
    if not _table_exists(con, "agg_stops_by_hour"):
        return CheckResult("hourly_rows_low", True, "info", "agg_stops_by_hour not built yet.")
    hours = _rows(con, "SELECT hour_utc, rows_total FROM agg_stops_by_hour ORDER BY hour_utc")
    if len(hours) < 3:
        return CheckResult("hourly_rows_low", True, "info", "Fewer than 3 hours of data.")
    normal = statistics.median(r[1] for r in hours)
    low = [(h.isoformat(), n) for h, n in hours if n < s.low_volume_ratio * normal]
    details = {"normal_rows_per_hour": normal, "low_hours": low}
    if low:
        listed = ", ".join(f"{h} ({n} rows)" for h, n in low)
        return CheckResult(
            "hourly_rows_low",
            False,
            "warning",
            f"{len(low)} hour(s) far below the normal {normal:g} rows/hour: {listed}.",
            details,
        )
    return CheckResult("hourly_rows_low", True, "info", "Every hour has normal volume.", details)


def check_failed_checks(state_dir: Path, dbt_target: Path) -> CheckResult:
    problems: list[str] = []
    details: dict[str, Any] = {}

    run_status = state_dir / "pipeline_run_latest.json"
    if run_status.exists():
        run = json.loads(run_status.read_text())
        details["pipeline_run"] = run
        if run.get("status") == "failed":
            problems.append(f"pipeline run {run.get('run_id')} failed at {run.get('failed_task')}")

    report = state_dir / "checks" / "success_check_latest.json"
    if report.exists():
        rep = json.loads(report.read_text())
        details["success_check"] = {
            "passed": rep.get("passed"),
            "checked_at": rep.get("checked_at"),
        }
        if not rep.get("passed"):
            problems.append("success test failed: " + "; ".join(rep.get("failures", [])))
    else:
        problems.append("success test has never run")

    results = dbt_target / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        bad = [
            r["unique_id"] for r in rr.get("results", []) if r.get("status") in ("fail", "error")
        ]
        details["dbt_failures"] = bad
        if bad:
            problems.append(f"{len(bad)} dbt node(s) failed: {', '.join(bad[:5])}")

    if problems:
        return CheckResult("check_failed", False, "critical", " | ".join(problems), details)
    return CheckResult("check_failed", True, "info", "All checks passed.", details)


def check_stream_silence(stream_db: Path, now: datetime, s: Settings) -> CheckResult:
    if not stream_db.exists():
        return CheckResult(
            "stream_silent", False, "warning", f"Stream store {stream_db} not found."
        )
    con = duckdb.connect(str(stream_db), read_only=True)
    try:
        if not _table_exists(con, "stream_events"):
            return CheckResult("stream_silent", False, "warning", "No stream events stored yet.")
        ((last,),) = _rows(con, "SELECT max(received_at) FROM stream_events")
    finally:
        con.close()
    if last is None:
        return CheckResult("stream_silent", False, "warning", "No stream events stored yet.")
    silent_min = (now - last).total_seconds() / 60
    details = {"last_event_received_at": last.isoformat(), "minutes_silent": round(silent_min, 1)}
    if silent_min > s.max_stream_silence_minutes:
        return CheckResult(
            "stream_silent",
            False,
            "warning",
            f"No stream event for {silent_min:.1f} min (limit {s.max_stream_silence_minutes:g}).",
            details,
        )
    return CheckResult("stream_silent", True, "info", "Stream is receiving events.", details)


def run_checks(
    db_path: Path,
    state_dir: Path,
    dbt_target: Path,
    now: datetime | None = None,
    stream_db: Path | None = None,
    settings: Settings | None = None,
) -> list[CheckResult]:
    now = now or datetime.now(UTC)
    s = settings or Settings()
    results: list[CheckResult] = []
    if db_path.exists():
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            results += [
                check_data_arrival(con, now, s),
                check_load_volume(con, s),
                check_hourly_volume(con, s),
            ]
        finally:
            con.close()
    else:
        results.append(
            CheckResult(
                "data_stopped_arriving", False, "critical", f"Database {db_path} not found."
            )
        )
    results.append(check_failed_checks(state_dir, dbt_target))
    if stream_db is not None:
        results.append(check_stream_silence(stream_db, now, s))
    return results


def alert_on(results: list[CheckResult], s: Settings, now: datetime) -> int:
    sent = 0
    for r in results:
        if r.ok:
            continue
        key = f"{r.name}:{now:%Y-%m-%d}"
        if send(Alert(r.name, r.severity, r.message, r.details, key), s.cooldown_seconds):
            sent += 1
    return sent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run pipeline health checks and send alerts.")
    p.add_argument("--db", type=Path, default=config.db_path())
    p.add_argument("--state-dir", type=Path, default=config.state_dir())
    p.add_argument("--dbt-target", type=Path, default=config.PROJECT_ROOT / "dbt" / "target")
    p.add_argument("--stream", action="store_true", help="also check the stream store")
    p.add_argument("--fail-on-alert", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from robot_pipeline.stream.consumer import stream_db_path

    now = datetime.now(UTC)
    s = Settings()
    results = run_checks(
        args.db, args.state_dir, args.dbt_target, now, stream_db_path() if args.stream else None, s
    )
    for r in results:
        print(f"{'OK  ' if r.ok else 'FAIL'} {r.name:<22} {r.message}")
    sent = alert_on(results, s, now)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    (args.state_dir / "monitor_latest.json").write_text(
        json.dumps(
            {
                "checked_at": now.isoformat(),
                "alerts_sent": sent,
                "results": [asdict(r) for r in results],
            },
            indent=2,
            default=str,
        )
    )
    from robot_pipeline.alerts import alert_log_path

    print(f"{sent} alert(s) sent; log: {alert_log_path()}")
    failed = [r for r in results if not r.ok]
    return 1 if (args.fail_on_alert and failed) else 0


if __name__ == "__main__":
    sys.exit(main())
