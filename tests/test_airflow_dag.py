"""Checks the Airflow DAG's structure. Skipped when Airflow is not installed
(the main venv does not include it; CI runs it in a separate job, which sets
REQUIRE_AIRFLOW=1 so a broken Airflow install fails instead of skipping)."""

import os
from datetime import timedelta
from pathlib import Path

import pytest

if not os.environ.get("REQUIRE_AIRFLOW"):
    pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "orchestration" / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    return bag.get_dag("robot_log_pipeline")


def test_dag_runs_daily_without_backfill(dag):
    assert dag is not None
    assert dag.schedule == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_steps_run_load_then_build_then_test(dag):
    assert [t.task_id for t in dag.topological_sort()] == [
        "load_raw_logs",
        "dbt_build",
        "success_check",
    ]
    assert dag.get_task("dbt_build").upstream_task_ids == {"load_raw_logs"}
    assert dag.get_task("success_check").upstream_task_ids == {"dbt_build"}


def test_every_step_retries_and_reports_failure(dag):
    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=5)
        assert task.on_failure_callback
        assert task.execution_timeout == timedelta(minutes=30)


def test_bad_retries_setting_falls_back_to_default(monkeypatch):
    import importlib.util

    monkeypatch.setenv("ROBOT_TASK_RETRIES", "not-a-number")
    spec = importlib.util.spec_from_file_location("dag_mod", DAGS / "robot_log_pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.dag.get_task("load_raw_logs").retries == 2


def test_paths_are_shell_quoted(monkeypatch, tmp_path):
    import importlib.util

    odd = tmp_path / "dir with $(touch pwned) space"
    monkeypatch.setenv("ROBOT_PROJECT_DIR", str(odd))
    spec = importlib.util.spec_from_file_location("dag_mod2", DAGS / "robot_log_pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cmd = mod.dag.get_task("load_raw_logs").bash_command
    assert cmd.startswith("cd '")  # single-quoted: $( ) is not evaluated


def test_run_status_file_is_written(monkeypatch, tmp_path):
    import importlib.util
    import json

    monkeypatch.setenv("ROBOT_STATE_DIR", str(tmp_path))
    spec = importlib.util.spec_from_file_location("dag_mod3", DAGS / "robot_log_pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.write_run_status({"run_id": "r1"}, "failed")
    record = json.loads((tmp_path / "pipeline_run_latest.json").read_text())
    assert record["status"] == "failed" and record["run_id"] == "r1"
