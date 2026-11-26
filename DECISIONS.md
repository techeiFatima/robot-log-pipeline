# Decisions

Every decision made on the author's behalf while building this project,
each with a one-line reason. Newest phase at the bottom.

## Phase 0: environment and data access

- **Downloaded the dataset from the official UCI zip** (`https://archive.ics.uci.edu/static/public/963/ur3+cobotops.zip`, contains `dataset_02052023.xlsx`). Reason: the file was not in the build environment, and the brief says to download it from UCI.
- **Verified all ten profiling findings with queries before using them.** Reason: the brief requires it; every finding matched (details in WALKTHROUGH.md, Phase 0).

## Phase 1: project setup

- **Pinned `duckdb==2.0.1` and `pandas==3.1.1`** to match the author's laptop; **`openpyxl==3.1.5`** is the current release. Reason: identical installs everywhere.
- **Split requirements into `requirements.txt` (runtime) and `requirements-dev.txt` (pytest, ruff).** Reason: production installs should not carry test tools.
- **Ruff for both linting and formatting.** Reason: one fast tool instead of flake8, isort and black.
- **Ignore `*.xlsx`, `*.csv`, `*.parquet` and `*.duckdb` everywhere, not only in `data/`.** Reason: a data file saved in the wrong folder must still never reach Git.
- **Branches named `claude/phase-NN-<name>`, merged with merge commits (not squash or rebase).** Reason: keeps every small commit visible in `main` and never rewrites history.

## Phase 2: loader

- **Loader logic moved into the package `robot_pipeline/`; `ingest.py` is now a thin command-line wrapper.** Reason: tests, Airflow and the stream consumer can import the same code.
- **No duplicates on re-run: rows from a file are deleted and re-inserted in one transaction, keyed on `source_file`.** Reason: simplest rule that is correct when a file is re-delivered with corrections, unlike "skip if already loaded".
- **Raw column types fixed in code (the types DuckDB inferred from the real file), not re-inferred per file.** Reason: a future file cannot silently change a column's type; values are still stored unchanged.
- **Column check compares exact names (spaces and case included) and also catches duplicate headers; column order is not enforced.** Reason: order does not change meaning, but a renamed column does.
- **Added a `load_audit` table (file, SHA-256, rows loaded, rows replaced, time).** Reason: monitoring needs a record of when data last arrived.
- **`loaded_at` is UTC (`TIMESTAMPTZ`); `source_file` is the file name, not the full path.** Reason: the name is stable across laptops, CI and S3; the path is not.
- **A `raw_logs` table from the old script (no metadata columns) is rebuilt automatically, with a warning.** Reason: it is a pure copy of the file, so nothing is lost.
- **By default the loader loads every `.xlsx` in `data/raw/`, not just the first one.** Reason: the old `glob(...)[0]` silently ignored other files, and a daily schedule would add files.
- **Removed the profiling printouts from `ingest.py`.** Reason: profiling lives in `explore.py`; the loader should only load. `explore.py` now opens the database read-only.
- **Paths come from environment variables `ROBOT_RAW_DIR`, `ROBOT_DB_PATH` and `ROBOT_STATE_DIR`, with local defaults.** Reason: CI, Airflow and the cloud setup can reuse the code unchanged.
