"""Print the project's headline numbers, straight from the database.

Every number in README.md and docs/ comes from this command (or the success
test), so the docs can be re-checked at any time:

    python -m robot_pipeline.report            # human-readable
    python -m robot_pipeline.report --json     # machine-readable
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

from robot_pipeline import config

QUERIES = {
    "raw_rows": "SELECT count(*) FROM raw_logs",
    "raw_columns": "SELECT count(*) - 2 FROM (DESCRIBE raw_logs)",  # minus loader metadata
    "first_event_utc": "SELECT min(event_ts)::VARCHAR FROM stg_robot_logs",
    "last_event_utc": "SELECT max(event_ts)::VARCHAR FROM stg_robot_logs",
    "span_hours": "SELECT round(date_diff('ms', min(event_ts), max(event_ts)) / 3.6e6, 2) "
    "FROM stg_robot_logs",
    "hours_recorded": "SELECT round(sum(seconds_since_previous) FILTER (WHERE NOT is_after_gap)"
    " / 3600, 2) FROM int_robot_log_sequence",
    "median_seconds_between_rows": "SELECT median(seconds_since_previous) "
    "FROM int_robot_log_sequence",
    "recording_gaps_over_5s": "SELECT count(*) FROM int_robot_log_sequence WHERE is_after_gap",
    "longest_gap_hours": "SELECT round(max(seconds_since_previous) / 3600, 2) "
    "FROM int_robot_log_sequence",
    "timestamps_with_stray_quotes": "SELECT count(*) FROM stg_robot_logs WHERE had_stray_quotes",
    "no_stop_rows": "SELECT count(*) FROM stg_robot_logs WHERE stop_status = 'no_stop'",
    "stop_rows": "SELECT count(*) FROM stg_robot_logs WHERE stop_status = 'stop'",
    "unknown_status_rows": "SELECT count(*) FROM stg_robot_logs WHERE stop_status = 'unknown'",
    "grip_lost_rows": "SELECT count(*) FROM stg_robot_logs WHERE grip_lost",
    "stop_episodes": "SELECT count(*) FROM fct_stop_events",
    "stop_episode_avg_seconds": "SELECT round(avg(duration_seconds), 2) FROM fct_stop_events",
    "stop_episode_max_seconds": "SELECT round(max(duration_seconds), 2) FROM fct_stop_events",
    "stop_episode_max_rows": "SELECT max(stop_rows) FROM fct_stop_events",
    "stop_episodes_with_grip_lost": "SELECT count(*) FROM fct_stop_events "
    "WHERE grip_lost_during_stop",
    "cycles": "SELECT count(*) FROM agg_stops_by_cycle",
    "cycles_with_a_stop": "SELECT count(*) FROM agg_stops_by_cycle WHERE had_stop",
    "cycles_in_two_stretches": "SELECT count(*) FROM agg_stops_by_cycle WHERE cycle_stretches > 1",
    "stop_episodes_per_recorded_hour": "SELECT round((SELECT count(*) FROM fct_stop_events) / "
    "(sum(seconds_since_previous) FILTER (WHERE NOT is_after_gap) / 3600), 1) "
    "FROM int_robot_log_sequence",
}

HOURLY = """
SELECT strftime(hour_utc, '%H:%M') AS hour_utc, rows_total, rows_unknown_status, stop_rows,
       stop_events, round(100 * stop_row_rate, 1) AS stop_row_pct,
       round(minutes_recorded, 1) AS minutes_recorded
FROM agg_stops_by_hour ORDER BY hour_utc
"""

CONDITIONS = """
SELECT stop_status,
       count(*) AS rows,
       round(avg((temperature_j0 + temperature_j1 + temperature_j2 + temperature_j3
                  + temperature_j4 + temperature_j5) / 6), 2) AS avg_joint_temp,
       round(avg(abs(current_j0) + abs(current_j1) + abs(current_j2) + abs(current_j3)
                 + abs(current_j4) + abs(current_j5)), 2) AS avg_total_abs_current,
       round(avg(abs(speed_j0) + abs(speed_j1) + abs(speed_j2) + abs(speed_j3)
                 + abs(speed_j4) + abs(speed_j5)), 3) AS avg_total_abs_speed,
       round(avg(tool_current), 3) AS avg_tool_current
FROM stg_robot_logs WHERE stop_status <> 'unknown' GROUP BY 1 ORDER BY 1
"""


def _records(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def collect(db_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        out = {k: con.execute(q).fetchall()[0][0] for k, q in QUERIES.items()}
        out["by_hour"] = _records(con, HOURLY)
        out["conditions"] = _records(con, CONDITIONS)
    finally:
        con.close()
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", type=Path, default=config.db_path())
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    data = collect(args.db)
    if args.json:
        print(json.dumps(data, indent=2, default=str))
        return 0
    for k, v in data.items():
        if isinstance(v, list):
            print(f"\n{k}:")
            for row in v:
                print("  " + "  ".join(f"{c}={x}" for c, x in row.items()))
        else:
            print(f"{k:<34} {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
