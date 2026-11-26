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

## Phase 3: dbt models

- **dbt Core 1.12.7 + dbt-duckdb 1.11.1, not dbt v2 (Fusion).** Reason: dbt's own docs say v2's built-in DuckDB adapter does not yet support every dbt-duckdb feature or DuckDB extensions; v1 is the lower-risk choice and still current.
- **`profiles.yml` lives in `dbt/` and reads the database path only from `ROBOT_DB_PATH`, which the wrapper `python -m robot_pipeline.dbt_runner` sets to an absolute path.** Reason: tested, dbt resolves a relative DuckDB path against the current directory, and running from the wrong folder silently created an empty database.
- **dbt's anonymous usage tracking is turned off.** Reason: no need to send data out; the call was also blocked by the network policy.
- **Made the package installable (`pip install -e .`, added to `requirements-dev.txt`).** Reason: `robot_pipeline` must import from any directory (dbt, Airflow, tests).
- **`event_ts` is a plain TIMESTAMP in UTC, not TIMESTAMPTZ.** Reason: every source value ends in `Z`; a time-zone type would shift hour buckets to the laptop's local zone.
- **The stop flag becomes both `protective_stop` (BOOLEAN, NULL when unknown) and `stop_status` ('stop' / 'no_stop' / 'unknown' / 'invalid').** Reason: the label makes "unknown" explicit and countable, and any unexpected source value shows up as 'invalid' and fails a test instead of being folded into "no stop".
- **The 54 unknown rows are kept with NULL stop values and `is_status_unknown = true`.** Reason: the brief forbids dropping or guessing them.
- **`grip_lost` is kept as delivered, even in unknown rows (always false there); documented as an open question.** Reason: setting it to NULL would also be a guess.
- **`Temperature_T0` renamed to `temperature_j0`, with the assumption written in the column description.** Reason: the brief asks to fix the pattern; the column docs keep it honest that the meaning is unconfirmed.
- **Negative current and speed values left unchanged.** Reason: their meaning (likely direction) is unconfirmed, and changing them would alter the data.
- **Row key `log_id = source_file:Num`.** Reason: `Num` alone repeats across files.
- **Order-dependent logic (gaps, stop episodes, cycle stretches) lives in an intermediate model, not staging.** Reason: staging stays a simple row-for-row cleanup that the success test compares against raw.
- **A stop episode is a run of consecutive 'stop' rows; any non-stop row, unknown row or gap longer than 5 s ends it.** Reason: 278 stop rows are really 108 separate events (most last 1–3 rows), and counting events answers "how often" better than counting rows.
- **Gap threshold 5 seconds (dbt var `gap_threshold_seconds`).** Reason: normal spacing is ~1.005 s, the largest non-gap step is 2.03 s, and the three real gaps are 9 s, 31 s and 5.2 h.
- **Hourly summary keyed on the full hour timestamp, not hour-of-day.** Reason: correct for one day today and for many days later.
- **Cycle summary counts `cycle_stretches`.** Reason: profiling found cycle numbers 224–227 and 229–233 each appear in two separate stretches of the log, so a cycle number is not one continuous run.
- **SQL uses dbt cross-database macros (`dbt.datediff`, `dbt.date_trunc`, `dbt.type_timestamp`) and `sum(case ...)` instead of DuckDB-only syntax.** Reason: the same models must run on Snowflake in Phase 9.
- **Staging and intermediate models are views; marts are tables.** Reason: dbt convention; the small views always reflect the latest raw data and the marts are what people query.
