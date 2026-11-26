"""Daily robot log pipeline: load -> build -> test -> monitor.

Each step runs one command of this project in the project's own virtualenv
(ROBOT_PYTHON), so Airflow's dependencies never clash with the pipeline's.
A step that exits non-zero fails; Airflow retries it, then marks it failed
and marks the steps after it "upstream_failed", so the first red box in the
UI is the step that broke. The run's final status is also written to
state/pipeline_run_latest.json, so an older passing check report is never
mistaken for the latest result.

Environment variables (all optional):
    ROBOT_PROJECT_DIR  repository root (default: two folders above this file)
    ROBOT_PYTHON       Python of the project's .venv (default: <root>/.venv/bin/python)
    ROBOT_TASK_RETRIES retries per step (default: 2; ignored if not a number)
    ROBOT_RAW_DIR, ROBOT_DB_PATH, ROBOT_STATE_DIR  passed through to the pipeline
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_DIR = Path(
    os.environ.get("ROBOT_PROJECT_DIR", Path(__file__).resolve().parents[2])
).resolve()
PYTHON = os.environ.get("ROBOT_PYTHON", str(PROJECT_DIR / ".venv" / "bin" / "python"))
STATE_DIR = Path(os.environ.get("ROBOT_STATE_DIR", PROJECT_DIR / "state"))

log = logging.getLogger(__name__)


def _retries(default: int = 2) -> int:
    """ROBOT_TASK_RETRIES, falling back to the default if unset or not a number,
    so a bad setting cannot stop the DAG from loading."""
    value = os.environ.get("ROBOT_TASK_RETRIES", "").strip()
    if not value:
        return default
    try:
        return max(0, int(value))
    except ValueError:
        log.warning("Ignoring ROBOT_TASK_RETRIES=%r (not an integer); using %d", value, default)
        return default


def write_run_status(context, status: str) -> None:
    """Record the whole run's outcome, so readers never mistake an older passing
    check report for the latest result."""
    dag_run = context.get("dag_run")
    ti = context.get("task_instance")
    record = {
        "dag_id": "robot_log_pipeline",
        "run_id": getattr(dag_run, "run_id", context.get("run_id")),
        "status": status,
        "failed_task": getattr(ti, "task_id", None) if status == "failed" else None,
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        (STATE_DIR / "pipeline_run_latest.json").write_text(json.dumps(record, indent=2))
    except OSError as exc:  # never let bookkeeping hide the real failure
        log.error("Could not write pipeline run status: %s", exc)


def report_failure(context) -> None:
    """Called once a task has failed for good (after its retries)."""
    ti = context["task_instance"]
    message = (
        f"robot_log_pipeline task '{ti.task_id}' FAILED after {ti.try_number} attempt(s) "
        f"for run {context.get('run_id')}. Logs: {getattr(ti, 'log_url', 'see Airflow UI')}"
    )
    log.error(message)
    # Send it through the project's alerting (log file + optional webhook).
    # robot_pipeline.alerts uses only the standard library, so it can be
    # imported here without adding any pipeline dependency to Airflow.
    try:
        if str(PROJECT_DIR) not in sys.path:
            sys.path.insert(0, str(PROJECT_DIR))
        from robot_pipeline.alerts import Alert, send

        send(
            Alert(
                name="pipeline_task_failed",
                severity="critical",
                message=message,
                details={"task_id": ti.task_id, "run_id": context.get("run_id")},
                dedup_key=f"{context.get('run_id')}:{ti.task_id}",
            ),
            cooldown_seconds=24 * 3600,
        )
    except Exception as exc:  # never let alerting hide the real failure
        log.error("Could not send the failure alert: %s", exc)


default_args = {
    "owner": "data-eng",
    # ROBOT_TASK_RETRIES lets a local failure test run without waiting for retries.
    "retries": _retries(),
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": report_failure,
}


def step(task_id: str, command: str, doc: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        **kwargs,
        # Paths are shell-quoted so a value with spaces or $(...) is never executed.
        bash_command=f"cd {shlex.quote(str(PROJECT_DIR))} && {shlex.quote(PYTHON)} {command}",
        append_env=True,
        doc_md=doc,
    )


with DAG(
    dag_id="robot_log_pipeline",
    description="Load robot logs, build dbt models and run the raw-vs-clean success test",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["robot-logs", "duckdb", "dbt"],
    doc_md=__doc__,
    on_success_callback=lambda ctx: write_run_status(ctx, "success"),
    on_failure_callback=lambda ctx: write_run_status(ctx, "failed"),
) as dag:
    load = step(
        "load_raw_logs",
        "ingest.py",
        "Load every .xlsx in the raw folder into raw_logs (idempotent).",
    )
    build = step(
        "dbt_build",
        "-m robot_pipeline.dbt_runner build",
        "Build the dbt models and run every dbt test; fails on any test failure.",
    )
    test = step(
        "success_check",
        "-m robot_pipeline.checks",
        "Raw-vs-clean success test: every source stop is a clean stop, unknowns counted.",
    )

    monitor = step(
        "monitor",
        "-m robot_pipeline.monitor --ignore-run-status",
        "Health checks (data stopped arriving, low row counts, failed checks); sends alerts. "
        "Runs even when an earlier step failed.",
        trigger_rule="all_done",
        retries=0,
    )
    # The run's final state comes from its leaf tasks. pipeline_ok is the only
    # leaf and needs every step to succeed, so a failed step always fails the
    # run, while monitor (all_done) still runs and alerts.
    pipeline_ok = EmptyOperator(task_id="pipeline_ok", doc_md="Green only if every step passed.")

    load >> build >> test >> monitor
    [test, monitor] >> pipeline_ok
