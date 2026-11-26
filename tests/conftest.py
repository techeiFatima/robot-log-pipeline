"""Shared pytest fixtures. All test data is SYNTHETIC (robot_pipeline.synthetic).

Imports of the pipeline happen inside the fixtures, so the Airflow-only test
environment (which has no pandas) can still collect tests/test_airflow_dag.py.
"""

from pathlib import Path

import pytest


@pytest.fixture
def sample_frame():
    from robot_pipeline.synthetic import make_frame

    return make_frame(rows=120, seed=7)


@pytest.fixture
def sample_file(tmp_path: Path) -> Path:
    from robot_pipeline.synthetic import write_sample

    return write_sample(tmp_path / "raw" / "SYNTHETIC_sample.xlsx", rows=120, seed=7)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.duckdb"
