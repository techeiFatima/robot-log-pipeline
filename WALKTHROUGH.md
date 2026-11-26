# Walkthrough

A plain-language tour of the project, one section per phase. Each
section explains every new file and design decision, the alternatives
that were rejected, and ends with three questions you should be able
to answer.

## Phase 0: checking the profiling findings

Before changing anything, the original `ingest.py` was run unchanged on
the downloaded file, and each of the ten profiling findings was checked
with a DuckDB query against `raw_logs`. All ten held:

| # | Finding | Query result |
|---|---|---|
| 1 | `Timestamp` is text | `DESCRIBE raw_logs` shows `VARCHAR` |
| 2 | Stray quotes | 863 values start and end with `"` (and no other value contains one) |
| 3 | One day, gaps, ~1.005 s spacing | min `2022-10-26 08:17:21.847`, max `15:36:06.580` (UTC, every value ends in `Z`); median spacing 1.005 s; 3 gaps longer than 5 s, the longest 18,814 s; non-gap time adds up to 2.08 hours |
| 4 | 54 nearly empty rows | 54 rows with NULL `Robot_ProtectiveStop`; in them every sensor is NULL except `Current_J0`, which is exactly 1.0 in 8 rows |
| 5 | Types and counts | stop: 7,077 × 0.0, 278 × 1.0, 54 × NULL; grip_lost: 243 true, 7,166 false |
| 6 | `"cycle "` | the column name has a trailing space |
| 7 | `Temperature_T0` | the only temperature column not named `Temperature_J<n>` |
| 8 | Mixed name styles | e.g. `Num`, `Current_J0`, `Tool_current`, `cycle `, `grip_lost` |
| 9 | Negatives | 7,290 rows have negative `Current_J1`; 2,399 have negative `Speed_J0` |
| 10 | grip_lost in empty rows | all 54 unknown rows have `grip_lost = false`, none NULL |

Two extra facts came out of the checks: `Num` runs from 1 to 7,409 with
no repeats, and timestamps never go backwards when sorted by `Num`.

## Phase 1: project setup

**What was added**

- `.gitignore`: now blocks data files by extension (`*.xlsx`, `*.csv`,
  `*.parquet`, `*.zip`) anywhere in the repo, plus database files
  (`*.duckdb`), secrets (`.env`, keys), Python caches, dbt output and the
  local Airflow and Kafka folders. The old file only blocked `data/` and
  one database name, so a copy of the dataset saved anywhere else would
  have been committable.
- `requirements.txt`: the packages the pipeline needs to run, each pinned
  to one exact version (`==`). Pinning means your laptop, CI and a
  reviewer all get the same code. DuckDB and pandas match your laptop.
- `requirements-dev.txt`: adds `pytest` (tests) and `ruff` (lint and
  format). It starts with `-r requirements.txt`, so one command installs
  both.
- `pyproject.toml`: one place for tool settings. Ruff checks style errors
  (`E`), real bugs like unused names (`F`), import order (`I`), common
  bug patterns (`B`) and outdated syntax (`UP`).
- `README.md`: a skeleton with the data disclaimer, setup steps using a
  `.venv`, and a status table that only claims what exists.
- `ingest.py`, `explore.py`: only formatting changed (import order, final
  newline), so the lint check passes from the start.

**Alternatives rejected**

- *Poetry or uv instead of pip and requirements files.* Both are good,
  but plain `pip` + `venv` needs nothing extra on your laptop and is what
  most interview reviewers can read instantly.
- *Version ranges like `pandas>=3`.* A range can install a different
  version tomorrow and break the pipeline silently.
- *A conda `environment.yml`.* You use miniconda, but CI and most teams
  use plain venvs; a venv works inside conda too.

**Questions you should be able to answer**

1. Why does `.gitignore` block `*.xlsx` everywhere instead of only `data/`?
2. What is the difference between `requirements.txt` and `requirements-dev.txt`, and why keep them separate?
3. What could go wrong if `requirements.txt` said `duckdb>=2` instead of `duckdb==2.0.1`?

## Phase 2: the loader

**What was added**

- `robot_pipeline/__init__.py`: makes `robot_pipeline` a Python package,
  so other code (tests, Airflow, the Kafka consumer) can
  `import robot_pipeline.loader`.
- `robot_pipeline/config.py`: the three paths the pipeline uses (raw
  folder, database file, state folder). Each reads an environment
  variable first (`ROBOT_RAW_DIR`, `ROBOT_DB_PATH`, `ROBOT_STATE_DIR`)
  and falls back to a local default. CI points them at a temporary
  folder without any code changes.
- `robot_pipeline/loader.py`: the loading logic.
  - `EXPECTED_COLUMNS` lists the 24 source columns, spelled exactly like
    the file (including `"cycle "` with its trailing space), and the
    type each is stored as. These are the types DuckDB picked for the
    real file, so the raw table looks just like before.
  - `read_source()` checks that the file exists, can be opened as Excel,
    has exactly the expected column names and is not empty. Each failure
    raises `LoaderError` with a sentence that tells you what to do.
  - `load_file()` opens one transaction, deletes any rows that came from
    a file with the same name, inserts the new rows with `source_file`
    and `loaded_at`, writes a row to `load_audit`, and commits. If
    anything fails, it rolls back, so the table is never half-loaded.
    That delete-then-insert is what makes it **idempotent**: running it
    twice gives the same result as running it once.
  - `load_audit` records every load (file name, SHA-256 checksum, rows
    loaded, rows replaced, time). Phase 8's "data stopped arriving"
    alert reads it.
- `ingest.py`: now a small command-line tool (`--file`, `--raw-dir`,
  `--db`). It exits with code 1 and one clear error line on failure,
  which is what Airflow and CI need to see a failed step.
- `explore.py`: opens the database read-only, so it can never change data.

**How it was verified (on the real file)**

- Two runs in a row: still 7,409 rows, 7,409 distinct `Num` values. The
  second run reports "replaced 7409".
- Every raw value equals what pandas reads from the Excel file (compared
  with `pandas.testing.assert_frame_equal`).
- A missing file, a renamed column (`cycle ` → `cycle`), a dropped
  column (`grip_lost`) and a missing folder each stop with exit code 1
  and a specific message.

**Alternatives rejected**

- *Skip a file if its checksum was already loaded.* Fast, but if the
  source re-sends a corrected file under the same name, the fix would
  be ignored. Replace-by-file handles both cases.
- *`CREATE OR REPLACE TABLE` (the old approach).* Idempotent, but it
  wipes rows from every other file, so it breaks once there is more than
  one daily file.
- *A primary key on `Num`.* `Num` is a row counter inside one file;
  two files could both have row 1, so it cannot be a global key.
- *Let DuckDB infer types each time.* A file where `grip_lost` has a
  blank cell would turn the column into text and break later steps
  without an error. Fixed types fail loudly instead.

**Questions you should be able to answer**

1. What exactly makes the loader safe to run twice, and why is it inside a transaction?
2. Why does the loader compare column names exactly, including the trailing space in `"cycle "`?
3. What is `load_audit` for, and which later phase depends on it?
