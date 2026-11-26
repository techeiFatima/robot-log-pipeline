"""Checks the Airflow DAG's structure. Skipped when Airflow is not installed
(the main venv does not include it; CI runs it in a separate job)."""

from datetime import timedelta
from pathlib import Path

import pytest

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
