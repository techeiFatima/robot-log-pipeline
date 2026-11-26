# Robot Log Pipeline

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
| dbt models, checks, CI, scheduling, streaming, monitoring, cloud, ML | Planned |

## Setup (macOS or Linux, Python 3.13)

```bash
git clone https://github.com/techeiFatima/robot-log-pipeline.git
cd robot-log-pipeline
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
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
python ingest.py                 # safe to repeat: rows are replaced, never duplicated
python ingest.py --file data/raw/dataset_02052023.xlsx --db cobot.duckdb
```

The loader stores the source values unchanged in `raw_logs`, adds
`source_file` and `loaded_at`, and records every load in `load_audit`.
It stops with a clear message if the file is missing, empty, or its
columns changed.

## What profiling found

- Timestamps are stored as text, and 863 of them are wrapped in stray quote marks
- 54 rows have no stop value and almost no sensor data
- Recording gaps: about 7.3 hours are covered by about 2 hours of rows

## Project documents

- `DECISIONS.md`: every decision made while building, each with a one-line reason
- `WALKTHROUGH.md`: a plain-language explanation of each phase
