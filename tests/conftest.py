"""Shared pytest fixtures. All test data is SYNTHETIC (robot_pipeline.synthetic)."""

from pathlib import Path

import pytest

from robot_pipeline.synthetic import make_frame, write_sample


@pytest.fixture
def sample_frame():
    return make_frame(rows=120, seed=7)


@pytest.fixture
def sample_file(tmp_path: Path) -> Path:
    return write_sample(tmp_path / "raw" / "SYNTHETIC_sample.xlsx", rows=120, seed=7)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.duckdb"
