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
  second run reported "replaced 7409". (Changed in Phase 6 after code
  review: an unchanged file is now skipped and reported as "Unchanged".)
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

## Phase 3: data models with dbt

dbt turns SQL `select` statements into tables and views, in dependency
order, and lets you document and test them. Everything lives in `dbt/`.

**What was added**

- `dbt/dbt_project.yml`: names the project, says staging and intermediate
  models are views and marts are tables, defines the variable
  `gap_threshold_seconds` (5), and turns off dbt's usage tracking.
- `dbt/profiles.yml`: how dbt connects. The `dev` target is the same
  DuckDB file the loader writes. It contains no secrets.
- `robot_pipeline/dbt_runner.py`: run dbt as
  `python -m robot_pipeline.dbt_runner build`. It passes an absolute
  database path, because dbt resolves a relative path against whatever
  folder you are in. Running from the repo root once silently created an
  empty `cobot.duckdb` one folder up; the wrapper prevents that.
- `dbt/models/staging/_sources.yml`: declares the loader's tables
  (`raw_logs`, `load_audit`) as **sources**, meaning data dbt reads but
  never changes. It also sets a freshness rule: warn if no load in 2
  days, error after 3.
- `dbt/models/staging/stg_robot_logs.sql`: the clean table. One row in,
  one row out. It strips the stray quotes and parses the timestamp
  (findings 1, 2), turns the 0/1 stop number into a boolean plus a label
  (5), renames `"cycle "` to `cycle_id` (6) and `Temperature_T0` to
  `temperature_j0` (7), makes every name snake_case (8), and flags the
  54 rows with no stop value as `unknown` instead of guessing (4). It also
  keeps `timestamp_raw` and `had_stray_quotes`, so you can always see the
  original value.
- `dbt/models/intermediate/int_robot_log_sequence.sql`: adds facts that
  depend on row order, using **window functions** (`lag`, running
  `sum`): seconds since the previous row, a gap flag, a stop-episode id
  and a cycle-stretch id. The "start of a group" flag summed as a running
  total is the classic **gaps-and-islands** technique.
- `dbt/models/marts/fct_stop_events.sql`: one row per stop episode, with
  start, end, duration, cycle, whether grip was lost, and average
  temperature, current and speed during the stop.
- `dbt/models/marts/agg_stops_by_hour.sql` and `agg_stops_by_cycle.sql`:
  the two summaries the brief asked for. Each counts total rows, known
  and unknown rows, stop rows and stop episodes separately.
- `dbt/macros/row_metrics.sql`: small reusable SQL snippets (average
  joint temperature, total absolute current, `count_if`), so the marts
  don't repeat long expressions.
- `_stg_models.yml`, `_int_models.yml`, `_marts.yml`: a description for
  every column. The data dictionary in Phase 10 is built from these.

**What the models show (from the real file)**

- 7,409 clean rows, the same as raw: 7,077 `no_stop`, 278 `stop`,
  54 `unknown`.
- The 278 stop rows form **108 stop episodes**. They last 1 to 12 rows,
  1.58 s on average and 11.07 s at most.
- By hour (UTC), episodes / rows: 08:00 → 16 / 2,494, 09:00 → 5 / 375,
  14:00 → 31 / 2,387, 15:00 → 56 / 2,153. The stop-row rate rises from
  2.1 % at 08:00 to 5.6 % at 15:00.
- 78 of 240 cycle numbers contain a stop.
- New finding: cycle numbers 224–227 and 229–233 each appear in two
  separate stretches of the log.

**Alternatives rejected**

- *Drop the 54 unknown rows in staging.* That is exactly the silent
  data loss the success test forbids.
- *Treat unknown as "no stop".* That would be a guess, and it would
  understate the stop rate if any of them were stops.
- *One big model.* Splitting staging (row cleanup), intermediate
  (ordering logic) and marts (answers) keeps each piece testable, and
  keeps the clean table a simple 1:1 copy of raw.
- *dbt v2 (Fusion).* Newer, but its DuckDB support is not yet complete.
- *Count stop rows instead of episodes.* One 12-second stop is one event,
  not twelve.

**Questions you should be able to answer**

1. Why does `stg_robot_logs` keep the 54 unknown rows, and how can a reader tell them apart?
2. How does the running `sum` of `is_stop_episode_start` turn 278 stop rows into 108 episodes?
3. Why is a cycle number not enough to identify one continuous cycle in this data?

## Phase 4: checks

There are three kinds of check, and each catches different problems.

**1. dbt generic tests** (in the `_*.yml` files, under `data_tests:`)

- `not_null` on every column that must always have a value (for example
  `event_ts`, `stop_status`, `grip_lost`).
- `unique` on every key (`log_id`, `stop_event_id`, `hour_utc`,
  `cycle_id` in the cycle summary).
- `accepted_values` on `stop_status`: only `stop`, `no_stop` or
  `unknown`. A source value like `2` becomes `invalid` in staging, and
  this test fails. A test proves this happens.

**2. The success test, as dbt singular tests** (`dbt/tests/*.sql`). A
singular test is a query that returns the bad rows; zero rows means pass.

- `assert_every_raw_stop_is_a_clean_stop.sql` joins raw to clean on
  `(source_file, Num)` and returns any raw stop that is not a clean stop,
  and any clean stop that raw does not have.
- `assert_unknown_status_rows_kept_and_counted.sql` does the same for
  rows with no stop value: each must appear in clean as `unknown`.
- `assert_clean_row_count_matches_raw.sql`: no row dropped or added.
- `assert_marts_reconcile_with_clean.sql`: the summaries add up to the
  clean table.

**3. The success test, as a Python report** (`robot_pipeline/checks.py`).
`python -m robot_pipeline.checks` runs one query that counts everything
from the database on that run, prints this, and writes
`state/checks/success_check_latest.json`:

```
Success test: raw_logs vs stg_robot_logs
  rows              raw   7409   clean   7409
  stop rows         raw    278   clean    278
  unknown status    raw     54   clean     54   (reported separately, never dropped or guessed)
  stops missing in clean 0, stops not in raw 0, invalid values 0
  RESULT: PASS
```

It exits with code 1 if anything fails, so Airflow and CI can stop on it.

**4. pytest unit tests** (`tests/`, 28 tests)

- `tests/conftest.py`: shared fixtures that create a SYNTHETIC Excel file
  in a temporary folder.
- `robot_pipeline/synthetic.py`: makes a fake file with the same columns
  and quirks as the real one (quoted timestamps, `"cycle "`, NULL stops,
  a time gap). The workbook title and sheet name say SYNTHETIC.
- `tests/test_loader.py`: the loader stores every value unchanged and
  adds metadata. **Running it twice gives 120 rows, not 240.** It also
  covers: a corrected file replaces its rows; two files stay separate; a
  missing file or folder, renamed, dropped, added or case-changed
  column, duplicate header, empty file and non-Excel file each fail
  clearly; a bad value rolls back and leaves the earlier load intact.
- `tests/test_checks.py`: builds a correct clean table, then breaks it
  one way per test (lose a stop, invent a stop, swap a stop with a
  non-stop so the totals still match, drop an unknown row, guess an
  unknown row). The check must fail each time.
- `tests/test_pipeline_integration.py`: loader → `dbt build` → check on a
  300-row synthetic file, plus a file with an invalid stop value that
  must make `dbt build` fail.

**Alternatives rejected**

- *Compare only total counts.* `test_a_swapped_stop_fails_even_when_totals_match`
  shows why that is not enough.
- *Hardcode 278 and 54 in the tests.* The brief forbids it, and it would
  break on any other file. Every count is queried on each run.
- *Great Expectations or Soda for data quality.* Capable tools, but dbt
  tests plus one SQL comparison cover every rule here with no extra
  framework to learn.
- *Commit a slice of the real data as a test fixture.* That would commit
  the dataset, which the brief forbids.

**Questions you should be able to answer**

1. Why does the success test join raw and clean row by row instead of comparing the two stop totals?
2. How do the tests prove the check fails when it should, not only that it passes?
3. What happens in `dbt build` if a new file contains `Robot_ProtectiveStop = 2`, and which test catches it?

## Phase 5: CI with GitHub Actions

CI (continuous integration) means every pull request is checked
automatically on a fresh machine. GitHub only *blocks* a merge on a red
check if the repository requires that check (Settings → Branches or
Rules → require status checks). That setting is not on yet; until it
is, the rule "only merge when CI is green" is followed by hand. Turning
it on is in your to-do list in FINAL_REPORT.md.

**What was added**

- `.github/workflows/ci.yml`: one workflow with three jobs that run at
  the same time on a clean Ubuntu machine with Python 3.13.
  - **Lint**: `ruff check` (code problems) and `ruff format --check`
    (formatting). `--output-format=github` makes errors show up as
    comments on the exact line of the PR.
  - **Unit and integration tests**: installs `requirements-dev.txt` and
    runs `pytest -v` (all 28 tests, which use synthetic data).
  - **dbt build on SYNTHETIC sample**: generates a 500-row synthetic file
    (random values, labelled SYNTHETIC in the file name, sheet name,
    workbook title and step names), loads it twice, runs `dbt build`
    (models + all tests) and then the success test. The check report is
    uploaded as a downloadable artifact of the run.
  - The three `ROBOT_*` environment variables point the pipeline at a
    temporary `ci_synthetic/` folder, so no code changes are needed for CI.
- Every action is pinned to a full commit SHA (with the version in a
  comment), not a tag like `@v7`. A tag can be moved to different code;
  a SHA cannot. `.github/dependabot.yml` opens a weekly PR when a newer
  version exists, so the pins don't go stale. (Suggested by the Sourcery
  review bot on the Phase 5 PR.)
- `.github/pull_request_template.md`: the checklist every PR description
  starts with, including "no data, database files or secrets".

**How it was verified**

Before pushing, the exact CI steps were run locally in a fresh venv, on a
clean copy of the repository (`git archive`): lint passed, 28 tests
passed, `dbt build` PASS=37, success test PASS (500 rows; 39 stops; 8
unknown). The real result is the green check on the Phase 5 pull request.

**Alternatives rejected**

- *Commit the synthetic file.* It would be a binary blob in Git that can
  drift from the generator; generating it takes a second.
- *Run CI on the real dataset by downloading it from UCI.* It would make
  CI depend on an outside website, and CI should not need the real data.
- *One job doing everything.* Slower, and a lint error would hide whether
  the tests pass.
- *pre-commit hooks only.* Useful locally, but they can be skipped; CI
  cannot.

**Questions you should be able to answer**

1. Why does CI use a generated synthetic file instead of the real dataset?
2. What does each of the three jobs catch that the others would not?
3. Why does the dbt job load the sample twice?

## Phase 6: scheduling with Airflow

Airflow runs workflows on a schedule and shows each step's state. A
workflow is a **DAG** (directed acyclic graph): steps with arrows between
them and no loops.

**What was added**

- `orchestration/dags/robot_log_pipeline.py`: the DAG.
  - Three steps: `load_raw_logs` (runs `ingest.py`), `dbt_build` (models
    and dbt tests), `success_check` (the raw-vs-clean test). The line
    `load >> build >> test` sets the order.
  - `schedule="@daily"` runs it once a day at midnight UTC.
    `catchup=False` means starting it today does not run every day since
    `start_date`. `max_active_runs=1` stops two runs writing the same
    database file at once.
  - `default_args` gives every step 2 retries (5 minutes apart, then
    longer), a 30-minute timeout and a failure callback.
  - **Failure states:** each script exits with code 1 on a problem.
    Airflow marks that step `failed` and the later steps
    `upstream_failed`, so the first red box is the cause.
  - Each step runs `cd <repo> && <repo>/.venv/bin/python ...`, so Airflow
    and the pipeline each keep their own dependencies.
- `orchestration/README.md`: exact steps to run it on your Mac without
  Docker (`airflow dags test` for one run, `airflow standalone` for the
  web UI).
- `tests/test_airflow_dag.py`: checks the DAG imports, runs daily without
  catch-up, has the right order, and every step has retries, a timeout and
  a failure callback. It is skipped in the main venv (no Airflow there)
  and runs in its own CI job.
- `tests/conftest.py`: its imports now happen inside the fixtures, so the
  Airflow-only test environment, which has no pandas, can load it.

**How it was verified (real runs, in this build environment)**

- `airflow dags test robot_log_pipeline` on the real file: the three steps
  succeeded in order (7,409 rows loaded, dbt PASS=37, success test PASS)
  and the run state was `success`.
- With an empty raw folder and retries set to 0: `load_raw_logs` →
  `failed` (message: "No .xlsx files in ..."), `dbt_build` and
  `success_check` → `upstream_failed`, run → `failed`, and the failure
  callback logged "task 'load_raw_logs' FAILED after 1 attempt(s)".
- DAG structure tests: 3 passed with Airflow 3.4.0.

**Not run:** the long-running scheduler and web UI (`airflow standalone`)
were not started here, only `airflow dags test`. Steps for your laptop are
in `orchestration/README.md`.

**Alternatives rejected**

- *`@task` Python functions that import the pipeline inside Airflow.* That
  puts pandas, DuckDB and dbt in Airflow's environment, where its
  constraints file pins other versions.
- *The official Docker Compose setup.* Fine for production-like setups,
  but it needs Docker (not available here) and is heavy on a laptop for a
  three-step DAG. The DAG file works unchanged there.
- *Cron.* Runs on schedule, but has no retries, no step states, no
  history and no UI.
- *A dbt-specific operator (for example Cosmos).* It shows each dbt model
  as its own Airflow task. That's useful at scale, but it adds a
  dependency for five models.

**Changes after code review**

The Sourcery review bot commented on the Phase 6 PR. Valid points were
fixed, and each fix has a test:

- *Stale data looked fresh:* the loader re-stamped an unchanged file with
  a new `loaded_at` every day. It now compares the file's SHA-256 with
  the last load and skips an identical file (`action = 'unchanged'` in
  `load_audit`). `python ingest.py --force` reloads anyway.
- *Half-finished multi-file loads:* all files in one run now load in a
  single transaction, after all of them pass validation.
- *Legacy table lost on failure:* the rebuild is now inside that
  transaction too.
- *Shell quoting:* configured paths are quoted with `shlex.quote`.
- *A bad `ROBOT_TASK_RETRIES` broke the DAG import:* it now falls back to 2.
- *An old passing report could look current:* the DAG writes
  `state/pipeline_run_latest.json` with the run's final status.
- *Docker or managed deployments:* documented in `orchestration/README.md`.

Not changed, with a reply on the PR: rows from a removed file are kept
(raw is an archive), and `REQUIRE_AIRFLOW=1` already fails rather than
skips (the bot misread it; verified by running it without Airflow).

Re-verified: three `airflow dags test` runs, real file. 1st: "Loaded",
status success. 2nd: "Unchanged", success. 3rd, with no files: status
failed, `failed_task: load_raw_logs`.

**Questions you should be able to answer**

1. If `dbt_build` fails, what state do `load_raw_logs` and `success_check` end up in, and why?
2. Why does each step call the project's `.venv` Python instead of importing the pipeline inside Airflow?
3. What does `catchup=False` prevent when you first turn the DAG on?

## Phase 7: real-time stream with Kafka

**This is a simulation.** The producer replays the recorded file as if
the robot were sending it live; nothing is connected to a real robot.

Kafka is a log of messages: **producers** append messages to a
**topic**, and **consumers** read them in order and remember how far
they got (their **offset**).

**What was added**

- `robot_pipeline/stream/events.py`: the message format. One JSON
  message per row, with `event_id = "<file>:<Num>"` as the dedup key and
  the 24 raw columns unchanged in `payload`.
- `robot_pipeline/stream/producer.py`: reads and validates the file (the
  loader's `read_source`), turns each row into an event and sends it.
  `--speed 1` waits between rows like the real timestamps (gaps capped
  at 5 s); `--speed 0` sends as fast as possible. `simulate_disorder()`
  can inject duplicates, swapped neighbours and late events.
- `robot_pipeline/stream/store.py`: the rules for messy delivery, kept
  separate from Kafka so they can be tested without a broker. See the
  table in `stream/README.md`. The key idea is the **watermark**: the
  newest event time seen so far. Anything older is out of order;
  anything more than 10 s older is late. Both are kept and flagged.
- `robot_pipeline/stream/consumer.py`: reads from Kafka, stores each
  event, and commits the offset only after the write. If it crashes
  between the write and the commit, the message comes again and the
  store ignores it.
- `stream/docker-compose.yml` and `stream/README.md`: how to run it on
  your laptop, with or without Docker.
- `tests/test_stream.py`: 13 tests. Duplicates, out-of-order, late,
  restart, bad messages and a full disordered replay all run without a
  broker. One end-to-end test uses a real broker (skipped if none is
  running; required in CI).
- CI job `kafka-stream-test`: starts `apache/kafka:4.4.0` as a service
  container on GitHub and runs the broker test.

**What was run here (real broker: Kafka 4.4.0, KRaft, Java 21)**

The full real file was replayed with injected problems
(`--speed 0 --duplicate-rate 0.02 --reorder-rate 0.02 --late-rate 0.01
--late-by 30 --seed 7`):

| | Count |
|---|---|
| Rows in the file | 7,409 |
| Messages sent (with injected duplicates) | 7,560 |
| Injected: duplicates / swaps / delayed | 151 / 154 / 80 |
| Events stored | 7,409 (each `event_id` once) |
| Duplicates ignored | 151 |
| Stored and flagged out of order (within 10 s) / late | 151 / 80 |
| Stop events stored / in raw | 278 / 278 |
| Unknown-status events stored / in raw | 54 / 54 |

One delayed event was 18,843 s late, because its delay crossed the
5.2-hour recording gap. It was still kept. Restarting the consumer read
0 new messages (offsets were committed) and changed no totals. A
6-row real-time replay took 6.3 s, about one row per second.

**Not run here:** the Docker Compose file (no Docker daemon in this
environment). The same image runs in the CI job.

**Alternatives rejected**

- *Drop late events (a fixed window).* Common in streaming, but the brief
  says never drop data. Flagging keeps the count honest.
- *Kafka exactly-once transactions.* Correct but complex. An idempotent
  store gives the same result here.
- *Write stream events into `cobot.duckdb`.* DuckDB allows one writer;
  the always-on consumer would lock out the daily batch.
- *kafka-python client.* Pure Python and simpler to install, but
  confluent-kafka is the more actively maintained and faster option.
- *Several partitions.* More throughput, but no ordering across
  partitions, and one robot doesn't need it.

**Changes after code review**

The Sourcery bot reviewed the Phase 7 PR. The most important find: after
an error, the consumer's final `commit()` could commit *past* the
message that failed, so after a restart that message was skipped. A new
test on the real broker made the consumer crash on message 50. With the
old code, the restarted consumer began at 51. The consumer now commits
explicit offsets, only up to the last message it fully handled, and the
test passes. Also fixed: only fatal Kafka errors stop the consumer;
duplicate and reject audit rows are unique per Kafka record (a
redelivery is not counted twice); all times are normalised to UTC; a
non-object payload is rejected; the wheel now includes
`robot_pipeline.stream`; the Docker broker binds to localhost only.
Kept by design, with replies on the PR: `event_id` stays
`<file>:<Num>`, and there is one consumer per `stream.duckdb`.

**Questions you should be able to answer**

1. Why is committing the offset *after* writing to DuckDB safe, even if the consumer crashes in between?
2. What is the watermark, and how does it decide whether an event is "out of order" or "late"?
3. Why does the stream use its own `stream.duckdb` file instead of `cobot.duckdb`?

## Phase 8: monitoring and alerting

**What was added**

- `robot_pipeline/alerts.py`: one function, `send(alert)`. It always
  appends the alert as a JSON line to `state/alerts.log`. If
  `ALERT_WEBHOOK_URL` is set, it also POSTs it there (Slack format with
  `ALERT_WEBHOOK_FORMAT=slack`). A broken webhook is recorded in the log,
  not raised. Alerts with the same `dedup_key` inside a cooldown are not
  repeated. It also works from the command line, which is how Airflow
  calls it.
- `robot_pipeline/monitor.py`: `python -m robot_pipeline.monitor` runs the
  health checks and sends an alert for each one that fails:
  - **data_stopped_arriving**: no new or changed data for 26 h.
  - **row_count_low**: the latest load is under half the usual size
    (needs 3 earlier loads to know "usual").
  - **hourly_rows_low**: an hour has under half the rows of a normal hour.
  - **check_failed**: the success test, a dbt test or the Airflow run failed.
  - **stream_silent** (`--stream`): the consumer received nothing for 5 min.
  It writes `state/monitor_latest.json` with every result.
- `robot_pipeline/stream/stop_alerts.py`: the **business alert**. Inside
  the Kafka consumer, when a stop row starts a new episode (no other stop
  from that robot in the previous 5 s), it sends a critical
  `protective_stop` alert with the time, the cycle, grip status and the
  measured delay.
- Airflow DAG: a `monitor` step after the checks that runs even if
  something failed (`all_done`), and an empty `pipeline_ok` step that is
  only green if everything passed. The run's final colour comes from its
  last steps, so without `pipeline_ok` a successful monitor would have
  hidden a failed load. Each failed step also sends `pipeline_task_failed`.
- `docs/runbooks.md`: what to do for each alert.
- `tests/test_alerts_monitoring.py`: 23 tests. Log and webhook (a real
  local HTTP server), Slack format, broken webhook, cooldown; each monitor
  check passing and failing, including "an unchanged re-load must not
  hide stale data"; the business alert (one per episode, no alert for
  duplicates or unknown rows, cooldown).

**What was run here, and the real results**

- `python -m robot_pipeline.monitor --stream` on the real data: 3 checks
  OK. `hourly_rows_low` fired: 09:00 UTC has 375 rows against a median
  of 2,270, because recording stopped ~6 minutes into that hour. (After
  the review, empty hours count too: 10:00–13:00 with 0 rows.)
  `stream_silent` fired because the replay had finished 10 minutes before.
- Business alert, consumer and producer running at the same time
  (1,500 rows at 10× speed): 8 alerts for 8 stop episodes (the batch
  table agrees), **median 0.016 s, max 2.2 s from send to alert**, all
  under the 5-minute target. The first alert waited for the consumer to
  find the new topic.
- Full file, sent first and consumed afterwards, with injected disorder:
  100 alerts, all under 5 minutes (max 32.9 s, inflated because the
  consumer only started after all 7,560 messages were sent). 100 = 99
  episodes under the 5-second rule + 1 extra from a stop that arrived out
  of order. The batch table counts 108 episodes because it splits on any
  non-stop row, even one only a second long.
- `airflow dags test`: success run → `success`. Run with no input files →
  `load_raw_logs failed`; `dbt_build`, `success_check` and `pipeline_ok`
  → `upstream_failed`; `monitor` ran; run → `failed`; a critical
  `pipeline_task_failed` alert was written to the log.

**Not run:** a real webhook service (Slack or similar). The webhook was
tested against a local HTTP server.

**Alternatives rejected**

- *Prometheus + Grafana + Alertmanager.* The industry standard, but it
  needs several running services. The checks here are written so their
  results could be exported to it later.
- *Airflow SLAs or email alerts only.* They cover the schedule, not data
  problems, and can't do a 5-minute stop alert.
- *dbt source freshness as the only "stopped arriving" check.* It is kept
  (warn 2 days, error 3), but it reads `loaded_at`, and the monitor's
  check is stricter (26 h) and knows about unchanged re-runs.
- *Alert on every stop row.* 278 alerts instead of ~100; one per episode
  is what a person needs.

**Changes after code review**

The review bot left 22 comments. The most important: if the consumer
stopped after storing a stop but before alerting, the redelivered message
counted as a duplicate and the alert was lost. Sent stop alerts are now
recorded in `stream.duckdb` (`stop_alerts_sent`), and a redelivered stop
without a recorded alert gets one. Other fixes, each with a test:
- a persistent cooldown, and tied timestamps count as one episode;
- a file lock and 7-day expiry for the cooldown state;
- no webhook redirects, and a failed webhook doesn't start the cooldown;
- empty hours count as low volume (the real data now shows 09:00–13:00);
- one loader run is compared as one batch;
- `--force` reloads are not "new data";
- a missing dbt results file is reported;
- thresholds that aren't finite numbers fall back to the defaults.
Two points were answered instead of changed (see DECISIONS.md). The
Airflow failure callback now imports the alert code directly instead of
starting a subprocess.
Re-measured: 8 alerts for 8 episodes, median 0.016 s, max 1.3 s.

**Questions you should be able to answer**

1. Why does the business alert live in the Kafka consumer instead of the daily Airflow run?
2. Why does the DAG need the empty `pipeline_ok` task?
3. Why would "data stopped arriving" never fire if the loader stamped a new `loaded_at` every time it re-read the same file?

## Phase 9: cloud (S3, Snowflake, Databricks)

**Status: NOT RUN, NEEDS ACCOUNTS.** No accounts, credentials or money
were used. The local DuckDB path is still the default. The full setup
steps are in `cloud/README.md`.

**What was added**

- `robot_pipeline/cloud/s3.py`: `upload` validates each file, stores the
  original under `raw/robot_logs/ingest_date=<day>/`, and stores a CSV copy
  under `staged/...` for Snowflake. An identical file is not uploaded
  twice. `download` brings a day's files back into `data/raw/`.
  Credentials come from your AWS profile, never from code.
- `cloud/aws/*.json`: least-privilege IAM policies, one for the pipeline
  and one for Snowflake to read the staged files.
- `dbt/profiles.yml`: a second target, `snowflake`, with key-pair auth
  and every value from an environment variable. Select it with
  `DBT_TARGET=snowflake`.
- `dbt/macros/parse_iso_utc_ts.sql`: the one place where DuckDB and
  Snowflake differ. dbt's `adapter.dispatch` picks the right version for
  each warehouse. Everything else already used dbt's cross-database
  macros.
- `cloud/snowflake/01_setup.sql` and `02_load_raw.sql`: create the
  warehouse, schemas, role, user, raw table, S3 stage, and the
  no-duplicates load.
- `cloud/databricks/`: a PySpark version of `stg_robot_logs` plus the
  success test (the job fails before writing if the test fails), and the
  bundle files to deploy it as a job.
- Tests: `tests/test_cloud_s3.py` (7 tests, against `moto`, a fake S3)
  and `tests/test_databricks_transform.py` (2 tests, on local Spark). Each
  has its own CI job.

**What was verified without accounts**

| Part | Verified | Not verified |
|---|---|---|
| S3 | 7 tests against moto (layout, unchanged bytes, CSV keeps values, idempotent, invalid never uploaded, download) | A real bucket and IAM |
| Snowflake | `dbt parse` on the Snowflake target; all 37 compiled SQL files parse in sqlglot's Snowflake dialect | `dbt compile/build` (needs a connection), the setup and load SQL |
| Databricks | The script on local Spark 4.3.0 on the real file: success test passed; output **identical to dbt's** on 30 columns × 7,409 rows | Running in a workspace, Delta, Unity Catalog |

**Alternatives rejected**

- *Snowpipe for continuous loading.* Good for streaming files, but more
  moving parts (SQS notifications) than one file a day needs.
- *dbt-external-tables package.* Declares S3 files as dbt sources, but
  adds a package; two short SQL scripts are easier to review.
- *Password auth for Snowflake.* Being deprecated for single-factor
  logins, and a password is easier to leak than a key file.
- *Databricks notebook instead of a Python file.* Notebooks are harder to
  test and review in Git. A plain `.py` file runs locally and in CI.
- *Run dbt on Databricks too (dbt-databricks).* Possible, but the brief
  asks for one transformation in Databricks, and a PySpark job shows a
  different skill than more dbt.

**Questions you should be able to answer**

1. Why does the S3 layout keep both the original `.xlsx` and a CSV copy?
2. Which single piece of SQL differs between DuckDB and Snowflake, and how does dbt choose the right version?
3. How was the Databricks transformation checked without a Databricks workspace, and what does that check *not* prove?

## Phase 10: documentation

**What was added**

- `README.md`, rewritten: what the project does and the data disclaimer
  (UCI dataset as a stand-in, no company affiliation, the stream is a
  simulation); a Mermaid architecture diagram; a status table that
  separates *built* from *run and verified here*; how to run; the data
  source; the ten profiling findings plus two new ones, with how each is
  handled; the real results; the success test and its real output; the
  stream and alert results.
- `robot_pipeline/report.py`: `python -m robot_pipeline.report` prints
  every headline number straight from the database. If the README ever
  disagrees with it, the README is wrong.
- `robot_pipeline/data_dictionary.py` and `docs/data_dictionary.md`:
  generated from the dbt column descriptions and the real column types
  in DuckDB, so it cannot drift. `docs/_data_dictionary_extra.md` holds
  the parts outside dbt (stream tables, state files).
- `docs/summary_for_non_engineers.md`: one page in plain language. What
  was found, how much to trust it, what was built.
- `docs/runbooks.md` (from Phase 8): one runbook per alert.
- Missing column descriptions were added to `_marts.yml`. A new test
  fails if any column in the dictionary has no description.

**How it was verified**

`python -m robot_pipeline.report` was run on the real data, and each
number in the README and the summary was copied from its output. The
integration test builds the synthetic database and runs both generators.

**Alternatives rejected**

- *`dbt docs generate` / `dbt docs serve` as the data dictionary.* It is
  useful, and still works (`python -m robot_pipeline.dbt_runner docs
  generate`), but it produces a website, not a file that reads well on
  GitHub, and it doesn't cover the stream tables.
- *A drawn PNG diagram.* It can't be diffed or reviewed in a PR.
- *Writing results by hand.* Easy to get wrong and impossible to re-check.

**Questions you should be able to answer**

1. If someone doubts a number in the README, how can they check it in one command?
2. Why is the data dictionary generated rather than written, and what stops it from going stale?
3. Why does the summary call the temperature pattern a "link, not proof of a cause"?
