# Robot Log Pipeline

[![CI](https://github.com/techeiFatima/robot-log-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/techeiFatima/robot-log-pipeline/actions/workflows/ci.yml)

A data engineering project that turns raw robot logs into clean,
tested tables, so a team can ask **how often a robot stops itself,
when, and under what conditions**, and be alerted when it happens.

> **About the data.** This project uses the public UR3 CobotOps dataset
> from the UCI Machine Learning Repository. It **stands in for real robot
> system logs**. This project is **not affiliated with any company**.

## Status

| Stage | State |
|---|---|
| Raw load and profiling (7,409 rows, 24 columns) | Done |
| Project setup (pinned requirements, `.venv`, `.gitignore`) | Done |
| Loader (idempotent, validated, audited) | Done |
| dbt models: clean table, stop episodes, summaries by hour and cycle | Done |
| Checks: dbt tests, raw-vs-clean success test, 28 pytest tests | Done |
| CI: lint, tests and dbt build on a SYNTHETIC sample, on every pull request | Done |
| Scheduling: daily Airflow DAG (load → build → test) with retries | Done (run with `airflow dags test`) |
| Streaming, monitoring, cloud, ML | Planned |

## Setup (macOS or Linux, Python 3.13)

```bash
git clone https://github.com/techeiFatima/robot-log-pipeline.git
cd robot-log-pipeline
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt   # also installs this package (-e .)
```

With miniconda, `python3.13` may not be on your PATH. If so, use the
conda interpreter: `python -m venv .venv` from an environment that has
Python 3.13 (`python --version` should print 3.13.x).

### Get the data

The dataset is not committed. Download it from
<https://archive.ics.uci.edu/dataset/963/ur3+cobotops> and put
`dataset_02052023.xlsx` in `data/raw/`:

```bash
mkdir -p data/raw
curl -L -o data/raw/ur3_cobotops.zip https://archive.ics.uci.edu/static/public/963/ur3+cobotops.zip
unzip data/raw/ur3_cobotops.zip -d data/raw
```

### Load it

```bash
python ingest.py                 # loads every .xlsx in data/raw/ into cobot.duckdb
python ingest.py                 # safe to repeat: unchanged files are skipped, never duplicated
python ingest.py --file data/raw/dataset_02052023.xlsx --db cobot.duckdb
```

The loader stores the source values unchanged in `raw_logs`, adds
`source_file` and `loaded_at`, and records every load in `load_audit`.
A changed file replaces its earlier rows; all files load in one
transaction (`--force` reloads unchanged files).
It stops with a clear message if the file is missing, empty, or its
columns changed.

### Build the models

```bash
python -m robot_pipeline.dbt_runner build
```

This creates `stg_robot_logs` (clean rows), `fct_stop_events` (one row
per stop episode), `agg_stops_by_hour` and `agg_stops_by_cycle` in the
same `cobot.duckdb`.

### Check it

```bash
python -m robot_pipeline.checks   # success test: raw vs clean, exits 1 on failure
pytest                            # unit + integration tests on SYNTHETIC data
pytest -m "not integration"       # quick run without dbt
```

### Schedule it (Airflow)

See [`orchestration/README.md`](orchestration/README.md).

## What profiling found

- Timestamps are stored as text, and 863 of them are wrapped in stray quote marks
- 54 rows have no stop value and almost no sensor data
- Recording gaps: about 7.3 hours are covered by about 2 hours of rows

## Project documents

- `DECISIONS.md`: every decision made while building, each with a one-line reason
- `WALKTHROUGH.md`: a plain-language explanation of each phase
