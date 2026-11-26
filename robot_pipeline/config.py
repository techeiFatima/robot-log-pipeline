"""Paths and settings, read from environment variables with local defaults.

Every setting can be overridden without editing code, which is how CI, Airflow
and the cloud setup point the same code at different files.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _path(env_name: str, default: Path) -> Path:
    value = os.environ.get(env_name)
    return Path(value).expanduser().resolve() if value else default


def raw_dir() -> Path:
    """Folder the loader reads Excel files from."""
    return _path("ROBOT_RAW_DIR", PROJECT_ROOT / "data" / "raw")


def db_path() -> Path:
    """DuckDB database file shared by the loader, dbt and the checks."""
    return _path("ROBOT_DB_PATH", PROJECT_ROOT / "cobot.duckdb")


def state_dir() -> Path:
    """Folder for runtime output: alert log, check reports, stream offsets."""
    return _path("ROBOT_STATE_DIR", PROJECT_ROOT / "state")
