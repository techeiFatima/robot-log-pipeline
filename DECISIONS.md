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

## Phase 4: checks

- **The success test exists twice: as dbt singular tests (so `dbt build` fails) and as `python -m robot_pipeline.checks` (which prints and saves the counts as JSON).** Reason: dbt stops bad data from reaching the summaries; the Python report gives monitoring and the docs real numbers to read.
- **Stops are matched row by row on `(source_file, Num)`, not only by comparing totals.** Reason: a test proves totals can match while a stop sits on the wrong row.
- **The check also fails on invented stops, not only lost ones.** Reason: "every source stop appears as a stop" alone would pass a table that marks everything as a stop.
- **Added a reconciliation test: stop events, hourly and per-cycle totals must add up to the clean table.** Reason: summaries that drift from the clean table would quietly give wrong answers.
- **Test arguments use the `arguments:` key and the flag `require_generic_test_arguments_property: true`.** Reason: current dbt docs; avoids the deprecation warning.
- **Tests use a generated SYNTHETIC file (`robot_pipeline/synthetic.py`) with the real file's quirks, never the real dataset.** Reason: tests must run anywhere, including CI, without the data, and the real data must never be committed.
- **The check tests write a fake clean table by hand and break it in one way each.** Reason: proves the check catches each failure, not only that it passes on good data.
- **End-to-end tests that run dbt are marked `integration`.** Reason: they are slower (~10 s), so they can be skipped during quick local runs.

## Phase 5: CI

- **Three parallel CI jobs: lint, pytest, and dbt build on a SYNTHETIC sample.** Reason: a failure points straight at the kind of problem.
- **The synthetic sample is generated during the CI run (500 rows, seed 42), not committed.** Reason: no data file in Git at all; the generator is the reviewable source of truth.
- **CI loads the sample twice before `dbt build`.** Reason: the run-twice guarantee is checked on every PR, not only in unit tests.
- **Runs on `pull_request`, on pushes to `main`, and on manual dispatch.** Reason: the brief asks for every PR; `main` runs confirm the merged result.
- **`permissions: contents: read` and a concurrency group that cancels superseded runs.** Reason: least privilege, and no wasted runs on rapid pushes.
- **Action versions `actions/checkout@v7`, `actions/setup-python@v7`, `actions/upload-artifact@v7`.** Reason: current major versions per their GitHub release pages (checked 2026-11-26).
- **Added a pull request template with a review checklist.** Reason: the target role asks for code reviews; the checklist includes "no data or secrets".
