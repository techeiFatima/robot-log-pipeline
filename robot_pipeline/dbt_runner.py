"""Run dbt with this project's settings from any working directory.

Usage:
    python -m robot_pipeline.dbt_runner build
    python -m robot_pipeline.dbt_runner test --select stg_robot_logs

It sets ROBOT_DB_PATH to an absolute path (dbt resolves a relative DuckDB path
against the current directory, which silently creates an empty database when
run from the wrong folder) and points dbt at the project and profiles in dbt/.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from robot_pipeline import config

DBT_DIR = config.PROJECT_ROOT / "dbt"


def dbt_executable() -> str:
    """The dbt next to the running Python (the .venv), else the one on PATH."""
    local = Path(sys.executable).with_name("dbt")
    if local.exists():
        return str(local)
    found = shutil.which("dbt")
    if not found:
        raise SystemExit("dbt is not installed. Run: pip install -r requirements.txt")
    return found


def run_dbt(args: list[str], db_path: Path | None = None) -> int:
    env = os.environ.copy()
    env["ROBOT_DB_PATH"] = str((db_path or config.db_path()).resolve())
    env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "False")
    cmd = [
        dbt_executable(),
        *args,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]
    return subprocess.call(cmd, env=env, cwd=DBT_DIR)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    return run_dbt(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
