# Final report

Built in one run on 2026-11-26: eleven phases, each on its own branch
with its own pull request, each merged only after its checks passed:
run locally for Phases 1–4, before CI existed, and green GitHub Actions
from Phase 5 on (PRs [#1](https://github.com/techeiFatima/robot-log-pipeline/pull/1)–[#11](https://github.com/techeiFatima/robot-log-pipeline/pull/11)); this report is PR #12.
Every number below came from a query or command that was actually run.
"Not run" means exactly that.

## Per phase: built / run and verified / not run

### 1. Project setup ([PR #1](https://github.com/techeiFatima/robot-log-pipeline/pull/1))
- **Built:** pinned `requirements*.txt`, a complete `.gitignore` (data, databases, secrets anywhere in the repo), `pyproject.toml` (ruff, pytest), a README skeleton with `.venv` setup, `DECISIONS.md`, `WALKTHROUGH.md`.
- **Run and verified:** fresh venv install; ruff; the original `ingest.py` on the real file. All ten profiling findings were re-checked by query; all held.
- **Not run:** nothing.

### 2. Loader ([PR #2](https://github.com/techeiFatima/robot-log-pipeline/pull/2), hardened in [#6](https://github.com/techeiFatima/robot-log-pipeline/pull/6))
- **Built:** `robot_pipeline/loader.py` and `ingest.py`. Validates all 24 column names exactly, adds `source_file` and `loaded_at`, keeps values unchanged, loads all files in one transaction, skips unchanged files (SHA-256), replaces changed ones, and records every load in `load_audit`.
- **Run and verified:** real file loaded twice → 7,409 rows, not 14,818. Stored values equal the Excel file (`assert_frame_equal`). Missing file, renamed, dropped or added columns, empty file, and bad values each fail with a clear message, and the earlier data stays intact.
- **Not run:** nothing.

### 3. dbt models ([PR #3](https://github.com/techeiFatima/robot-log-pipeline/pull/3))
- **Built:** `stg_robot_logs` (fixes findings 1, 2, 5, 6, 7, 8; flags the 54 unknown rows), `int_robot_log_sequence`, `fct_stop_events`, `agg_stops_by_hour`, `agg_stops_by_cycle`, all on DuckDB.
- **Run and verified:** `dbt build` on the real file. 278 stop rows form 108 episodes. A new finding: cycle numbers 224–227 and 229–233 each appear in two separate stretches.
- **Not run:** nothing.

### 4. Checks ([PR #4](https://github.com/techeiFatima/robot-log-pipeline/pull/4))
- **Built:** dbt tests (`not_null`, `unique`, `accepted_values`); the success test as dbt singular tests and as `python -m robot_pipeline.checks`; a SYNTHETIC data generator; pytest suites.
- **Run and verified:** success test on the real file: rows 7,409 / 7,409; stops 278 / 278; unknown 54 / 54; PASS. Tests prove it fails on a lost, invented, swapped, dropped or guessed row. At the end: **104 pytest tests pass** locally (2 more run only in their own CI jobs), and `dbt build` gives PASS=43.
- **Not run:** nothing.

### 5. CI ([PR #5](https://github.com/techeiFatima/robot-log-pipeline/pull/5))
- **Built:** GitHub Actions on every PR. It started with 3 jobs and has 7 now: lint, pytest, dbt build on a SYNTHETIC sample, Airflow DAG, Kafka with a real broker, Spark, and S3 on moto. Actions are pinned to commit SHAs, Dependabot is on, and there is a PR template.
- **Run and verified:** every PR from #5 onward was merged only with all jobs green.
- **Not run:** nothing. Note that GitHub does not *enforce* green checks until you turn on branch protection (to-do 1).

### 6. Scheduling with Airflow ([PR #6](https://github.com/techeiFatima/robot-log-pipeline/pull/6))
- **Built:** `orchestration/dags/robot_log_pipeline.py`: `load → dbt build → success check → monitor`, `@daily`, 2 retries with backoff, a 30-minute timeout, failure alerts, and a `pipeline_ok` leaf task so a failed step always fails the run.
- **Run and verified:** Airflow 3.4.0 `airflow dags test` on the real file → `success`. With no input file: `load_raw_logs failed`, downstream steps `upstream_failed`, `monitor` still ran, run state `failed`, and a critical alert was written. DAG tests run in CI.
- **Not run:** the long-running scheduler and web UI (`airflow standalone`), and the Docker Compose deployment. Steps are in `orchestration/README.md`.

### 7. Kafka stream, simulated ([PR #7](https://github.com/techeiFatima/robot-log-pipeline/pull/7))
- **Built:** a producer that replays the file in real time or fast, and can inject duplicates, swaps and late events; a consumer with explicit offset commits; a DuckDB store that dedups by `event_id` and keeps and flags late or out-of-order events; Docker Compose; docs stating it is a simulation.
- **Run and verified:** a real Kafka 4.4.0 broker (KRaft, from the Apache tarball, checksum checked). The full file was replayed with disorder: 7,560 messages → 7,409 events stored once, 151 duplicates ignored, 231 out-of-order (80 late) events kept. Stops 278 and unknown 54 both match raw. A restart re-read nothing. A test proves a message that fails mid-processing is re-read, not skipped. The `apache/kafka:4.4.0` image runs in CI.
- **Not run:** `docker compose` on a laptop (no Docker daemon here; CI covers the image).

### 8. Monitoring and alerting ([PR #8](https://github.com/techeiFatima/robot-log-pipeline/pull/8))
- **Built:** alerts to `state/alerts.log` by default, with an optional webhook (`ALERT_WEBHOOK_URL`, Slack format available). Checks: data stopped arriving, row counts far below normal (per load and per hour, including empty hours), failed checks, stream silent. A **business alert within 5 minutes of a protective stop**, raised by the stream consumer, retried after a crash and recorded in the database. Runbooks in `docs/runbooks.md`.
- **Run and verified:** on the real data, `hourly_rows_low` fires for 09:00–13:00 UTC (the recording gap). Stop alerts, consumer and producer running at the same time: 8 alerts for 8 episodes, a median of 0.016 s and at most 2.2 s from send to alert. The webhook was tested against a local HTTP server, including the redirect refusal.
- **Not run:** a real external webhook (Slack or similar).

### 9. Cloud: S3, Snowflake, Databricks ([PR #9](https://github.com/techeiFatima/robot-log-pipeline/pull/9), fixes in [#11](https://github.com/techeiFatima/robot-log-pipeline/pull/11)): **NOT RUN, needs accounts**
- **Built:** S3 upload and download (validated, idempotent, least-privilege IAM policies); a Snowflake dbt target with key-pair auth plus setup and load SQL; a Databricks PySpark job and bundle.
- **Verified without accounts:** S3 against moto (7 tests, in CI). `dbt parse` succeeds on the Snowflake target, and all compiled SQL parses in sqlglot's Snowflake dialect. The PySpark job ran on local Spark 4.3.0 on the real file and its output is identical to dbt's on 30 columns × 7,409 rows.
- **Not run:** anything against real AWS, Snowflake or Databricks. `dbt compile` and `dbt build` on Snowflake need a connection. The Snowflake SQL was written from the docs, then reviewed and fixed, but has never been executed.

### 10. Documentation ([PR #10](https://github.com/techeiFatima/robot-log-pipeline/pull/10))
- **Built:** the final README (architecture diagram, how to run, data source, findings, results, success test); a generated data dictionary; runbooks; a one-page summary for non-engineers; `python -m robot_pipeline.report` as the single source of every number.
- **Run and verified:** the README instructions were followed in a fresh clone and a fresh venv with the real file: dbt PASS=43, success test PASS, monitor, report and training all worked.
- **Not run:** nothing.

### 11. Stop-prediction model ([PR #11](https://github.com/techeiFatima/robot-log-pipeline/pull/11))
- **Built:** `ml_stop_features` (dbt, look-back features, label "a stop starts within 10 s"); time-based training and evaluation against two baselines.
- **Run and verified:** gradient-boosted trees: PR-AUC 0.325 (95 % CI 0.260–0.407) against a base rate of 0.209. They rank risk better than chance, but at the threshold chosen on validation they warn for only 10 of 57 test stops (32 false alarms per recorded hour). **Real signal, not a usable warning.** A no-look-ahead test, shown to catch an injected leak, passes. An independent review found two evaluation bugs that had made results look better (46 of 62); both were fixed before merge.
- **Not run:** nothing.

## Code review
- Phases 5–8 were reviewed by the Sourcery bot (2, 9, 10 and 22 comments). Each comment was fixed or answered on the PR; the most important fixes were a Kafka offset bug and a lost-stop-alert bug.
- Its weekly budget then ran out (upgrading costs money, so not done). Phases 9–11 got an independent review by a separate agent, which found 6 real bugs; all were fixed in PR #11.

## Every decision made on your behalf

One line each below. The reason for each is in [`DECISIONS.md`](DECISIONS.md).

### Phase 0: environment and data access

- Downloaded the dataset from the official UCI zip
- Created a personal cloud environment named `robot-log-pipeline` for this build session that allows only `archive.ics.uci.edu`.
- Ran Kafka from the official Apache tarball and Airflow from pip in this build environment.
- Verified all ten profiling findings with queries before using them.

### Phase 1: project setup

- Pinned `duckdb==2.0.1` and `pandas==3.1.1`
- Split requirements into `requirements.txt` (runtime) and `requirements-dev.txt` (pytest, ruff).
- Ruff for both linting and formatting.
- Ignore `*.xlsx`, `*.csv`, `*.parquet` and `*.duckdb` everywhere, not only in `data/`.
- Branches named `claude/phase-NN-<name>`, merged with merge commits (not squash or rebase).

### Phase 2: loader

- Loader logic moved into the package `robot_pipeline/`; `ingest.py` is now a thin command-line wrapper.
- No duplicates on re-run: rows from a file are deleted and re-inserted in one transaction, keyed on `source_file`.
- Raw column types fixed in code (the types DuckDB inferred from the real file), not re-inferred per file.
- Column check compares exact names (spaces and case included) and also catches duplicate headers; column order is not enforced.
- Added a `load_audit` table (file, SHA-256, rows loaded, rows replaced, time).
- `loaded_at` is UTC (`TIMESTAMPTZ`); `source_file` is the file name, not the full path.
- A `raw_logs` table from the old script (no metadata columns) is rebuilt automatically, with a warning.
- By default the loader loads every `.xlsx` in `data/raw/`, not just the first one.
- Removed the profiling printouts from `ingest.py`.
- Paths come from environment variables `ROBOT_RAW_DIR`, `ROBOT_DB_PATH` and `ROBOT_STATE_DIR`, with local defaults.

### Phase 3: dbt models

- dbt Core 1.12.7 + dbt-duckdb 1.11.1, not dbt v2 (Fusion).
- `profiles.yml` lives in `dbt/` and reads the database path only from `ROBOT_DB_PATH`, which the wrapper `python -m robot_pipeline.dbt_runner` sets to an absolute path.
- dbt's anonymous usage tracking is turned off.
- Made the package installable (`pip install -e .`, added to `requirements-dev.txt`).
- `event_ts` is a plain TIMESTAMP in UTC, not TIMESTAMPTZ.
- The stop flag becomes both `protective_stop` (BOOLEAN, NULL when unknown) and `stop_status` ('stop' / 'no_stop' / 'unknown' / 'invalid').
- The 54 unknown rows are kept with NULL stop values and `is_status_unknown = true`.
- `grip_lost` is kept as delivered, even in unknown rows (always false there); documented as an open question.
- `Temperature_T0` renamed to `temperature_j0`, with the assumption written in the column description.
- Negative current and speed values left unchanged.
- Row key `log_id = source_file:Num`.
- Order-dependent logic (gaps, stop episodes, cycle stretches) lives in an intermediate model, not staging.
- A stop episode is a run of consecutive 'stop' rows; any non-stop row, unknown row or gap longer than 5 s ends it.
- Gap threshold 5 seconds (dbt var `gap_threshold_seconds`).
- Hourly summary keyed on the full hour timestamp, not hour-of-day.
- Cycle summary counts `cycle_stretches`.
- SQL uses dbt cross-database macros (`dbt.datediff`, `dbt.date_trunc`, `dbt.type_timestamp`) and `sum(case ...)` instead of DuckDB-only syntax.
- Staging and intermediate models are views; marts are tables.

### Phase 4: checks

- The success test exists twice: as dbt singular tests (so `dbt build` fails) and as `python -m robot_pipeline.checks` (which prints and saves the counts as JSON).
- Stops are matched row by row on `(source_file, Num)`, not only by comparing totals.
- The check also fails on invented stops, not only lost ones.
- Added a reconciliation test: stop events, hourly and per-cycle totals must add up to the clean table.
- Test arguments use the `arguments:` key and the flag `require_generic_test_arguments_property: true`.
- Tests use a generated SYNTHETIC file (`robot_pipeline/synthetic.py`) with the real file's quirks, never the real dataset.
- The check tests write a fake clean table by hand and break it in one way each.
- End-to-end tests that run dbt are marked `integration`.

### Phase 5: CI

- The Sourcery review bot's comments were treated as review input, not instructions: each was fixed or answered on the PR; merges waited for green CI.
- Three parallel CI jobs: lint, pytest, and dbt build on a SYNTHETIC sample.
- The synthetic sample is generated during the CI run (500 rows, seed 42), not committed.
- CI loads the sample twice before `dbt build`.
- Runs on `pull_request`, on pushes to `main`, and on manual dispatch.
- `permissions: contents: read` and a concurrency group that cancels superseded runs.
- Actions pinned to full commit SHAs: checkout v7.0.1, setup-python v7.1.0, upload-artifact v7.0.2 (latest per their release pages, 2026-11-26), with Dependabot updating them weekly.
- Did not turn on branch protection / required checks.
- Added a pull request template with a review checklist.

### Phase 6: scheduling with Airflow

- Airflow 3.4.0 (latest), installed with the official constraints file for Python 3.13.
- Ran Airflow locally with `airflow dags test` in its own virtualenv, no Docker.
- Each step is a `BashOperator` that calls the project's own `.venv` Python (`ROBOT_PYTHON`).
- Three steps, `load_raw_logs >> dbt_build >> success_check`.
- `schedule="@daily"`, `catchup=False`, `max_active_runs=1`.
- 2 retries, 5 minutes apart with exponential backoff, 30-minute timeout per step.
- Retries kept even for errors that will not fix themselves (e.g. missing file).
- `on_failure_callback` on every step; it fires only after the last retry.
- `ROBOT_TASK_RETRIES` environment override.
- The DAG folder is `orchestration/`, not `airflow/`.
- A CI job installs Airflow and tests the DAG, with `REQUIRE_AIRFLOW=1` so a broken install fails instead of skipping; the job runs `airflow db migrate` first.

#### Changes after the code review on the Phase 6 PR (Sourcery bot)

- An unchanged file (same name and SHA-256, rows still present) is no longer reloaded; `load_audit.action` records `loaded`, `replaced` or `unchanged`; `--force` reloads anyway.
- All files of one run load in one transaction, and every file is validated before the database is touched.
- The legacy-table rebuild now happens inside that transaction.
- The DAG shell-quotes `ROBOT_PROJECT_DIR` and `ROBOT_PYTHON` and ignores a non-numeric `ROBOT_TASK_RETRIES`.
- DAG-level callbacks write `state/pipeline_run_latest.json` (success/failed + failed task).
- Not changed: rows from a file removed from `data/raw/` stay in `raw_logs`.
- Switched loader queries from `fetchone()` to a full-fetch helper.

### Phase 7: real-time stream with Kafka

- Kafka 4.4.0 in KRaft mode (no ZooKeeper), run from the official Apache tarball with Java 21 in the build environment; `apache/kafka:4.4.0` for Docker.
- Python client `confluent-kafka` 2.16.2, in its own `requirements-stream.txt`.
- The stream replays the raw rows unchanged inside `payload`, plus `event_id`, `robot_id` and a parsed `event_ts`.
- One topic `robot.logs.v1`, one partition, key = robot id.
- Producer uses `enable.idempotence=true` and `acks=all`.
- Duplicates: `event_id` primary key with `ON CONFLICT DO NOTHING`; repeats are logged in `stream_duplicates`.
- Late and out-of-order events are stored and flagged, never dropped; allowed lateness 10 s.
- Manual offset commits after the write (`enable.auto.commit=false`).
- The stream writes to a separate `stream.duckdb`.
- The consumer's idle timer starts after the first message, with a 30 s startup grace.
- CI runs the broker test against an `apache/kafka:4.4.0` service container, with `REQUIRE_KAFKA=1` so it cannot silently skip.
- Disorder injection is deterministic (seeded).

#### Changes after the code review on the Phase 7 PR (Sourcery bot)

- Offsets are committed explicitly, only up to the last fully handled message.
- Only fatal Kafka errors stop the consumer; others are logged and polling continues.
- `stream_duplicates` and `stream_rejects` are unique per (topic, partition, offset).
- All event times are normalised to naive UTC; `decode` rejects a non-object `payload`.
- The package config now includes `robot_pipeline.stream`
- The Docker Compose broker binds to `127.0.0.1` only.
- `REQUIRE_KAFKA=1` now also fails if the client library cannot be imported.
- Not changed: `event_id` stays `<file name>:<Num>`.
- Not changed: one consumer per `stream.duckdb` file.

### Phase 8: monitoring and alerting

- Alerts always go to `state/alerts.log` (JSON lines); `ALERT_WEBHOOK_URL` adds an HTTP POST; `ALERT_WEBHOOK_FORMAT=slack` sends Slack's `{"text": ...}` shape.
- `alerts.py` uses only the standard library, and a failing webhook is logged, never raised.
- Batch alerts repeat at most once per day per alert (cooldown keyed by name and date).
- "Data stopped arriving" uses the last load with new or changed content, threshold 26 h.
- "Row counts far below normal" is checked two ways: latest load vs the median of the last 10 loads (needs 3), and each hour vs the median hour; threshold 50 %.
- "Failed checks" reads the success-check report, dbt's `run_results.json` and the pipeline run status.
- The business alert lives in the stream consumer, not the daily batch.
- One business alert per stop episode: a stop with no other stop from the same robot in the previous 5 s of event time; an optional cooldown is off by default.
- The alert records `seconds_from_sent_to_alert`, measured from the producer's `sent_at`.
- An out-of-order stop that arrives before its predecessor can raise one extra alert.
- The consumer now treats "unknown topic" as "wait", not a crash.
- Airflow: a `monitor` task (`trigger_rule=all_done`) runs after the checks even when a step failed; an empty `pipeline_ok` task is the only leaf.
- Task failure callbacks send `pipeline_task_failed` through `robot_pipeline.alerts` (standard library only, imported from the project folder).

#### Changes after the code review on the Phase 8 PR (Sourcery bot, 22 comments)

- Sent stop alerts are recorded in `stop_alerts_sent` in the stream store; a redelivered stop without a recorded alert is alerted.
- The stop cooldown is read from that table, so it survives restarts; tied timestamps count as one episode.
- A malformed `sent_at` no longer blocks the alert (the delay is then unknown).
- Alert cooldown state: an exclusive file lock (`fcntl`, macOS/Linux) around check-and-record, entries expire after 7 days, the cooldown only starts when delivery worked, and stop alerts no longer use it.
- Webhook: no redirects followed; a malformed URL is logged as a failed delivery; control characters stripped from the logged text.
- Monitor: the batch cooldown is 24 h (once per day); thresholds must be finite and positive; files of one loader run are compared as one batch; hours with zero rows count as low; duplicates and rejects count as stream activity; a missing `run_results.json` is a problem; inside Airflow the monitor ignores the previous run's status file (`--ignore-run-status`).
- A forced reload of unchanged content is recorded as `reloaded` and does not count as new data.
- Not changed: a durable webhook outbox.
- Not changed: the episode lookup query.
- The DAG failure callback now imports `robot_pipeline.alerts` directly instead of running it as a subprocess.

### Phase 9: cloud (S3, Snowflake, Databricks): NOT RUN, needs accounts

- No accounts created, no credentials entered, nothing spent; everything is marked "NOT RUN, NEEDS ACCOUNTS".
- S3 layout `raw/robot_logs/ingest_date=YYYY-MM-DD/` (original `.xlsx`, byte-for-byte) and `staged/robot_logs/...` (CSV of the same rows plus `source_file`).
- Files are validated with the loader's rules before upload; re-upload of identical content is skipped (SHA-256 in object metadata).
- Credentials only through boto3's default chain (SSO profile or IAM role); least-privilege policies limited to the two prefixes.
- S3 code tested with `moto` (an in-memory S3 fake), locally and in a CI job.
- Snowflake: key-pair auth for a `TYPE = SERVICE` user, a role scoped to the two schemas, XSMALL warehouse with 60 s auto-suspend.
- Snowflake raw table uses the exact quoted source column names (including `"cycle "`), so the same dbt models run on both targets unchanged.
- Only one engine-specific macro: `parse_iso_utc_ts` (dispatch), with an explicit format on Snowflake.
- The Snowflake load copies into a temporary table, then deletes and inserts by `source_file` in one transaction.
- Snowflake verified only by `dbt parse` on the Snowflake target and by parsing all 37 compiled SQL files with sqlglot's Snowflake dialect.
- Databricks transformation = the staging cleanup plus the success test in PySpark, which fails the job before writing if the test fails.
- Deployed as a Declarative Automation Bundle (formerly Asset Bundles) with one serverless Python task.
- The PySpark code was run on local Apache Spark 4.3.0 on the real file and compared with dbt's output: identical on all 30 shared columns.
- PySpark is not in any requirements file for the main venv; CI installs it in its own job.

### Phase 10: documentation

- Every number in the README and the summary comes from `python -m robot_pipeline.report` (or the success-check output), not typed by hand.
- The data dictionary is generated (`python -m robot_pipeline.data_dictionary`) from the dbt YAML descriptions plus the live column types, and a test fails if any column lacks a description.
- Columns that pass through unchanged inherit their staging description in the dictionary.
- Architecture diagram in Mermaid inside the README.
- The non-engineer summary states the temperature finding as an association, not a cause.
- The README status table separates "built" from "run and verified here".
- One Phase 10 commit is labelled "WIP".

### Phase 11: stop-prediction model

- Target: "will a new stop episode start within the next 10 s?", on rows not already stopped and with known status.
- Features built in SQL as the dbt model `ml_stop_features` (current values, 10-row rolling stats, time since the last stop, stops in the last 300 rows, position in the cycle).
- No hour-of-day feature.
- Rows whose 10-second label window is cut by a gap or the end of the file are excluded.
- Time-based split: earliest 70 % of rows train, latest 30 % test, with training rows within 10 s of the split dropped.
- Models fit on the first 80 % of the training period; threshold (best F1) and best model chosen on the last 20 %; the same fitted model scored once on test.
- PR-AUC as the main metric, with a 1-minute block bootstrap 95 % interval; also episode-level recall and false-alarm runs per recorded hour.
- Baselines: base rate and a "recent stop" rule; models: logistic regression and gradient-boosted trees (scikit-learn 1.9.1, fixed seeds).
- A test proves features never look ahead (cut the source after the compared rows, rebuild, features unchanged); it was checked by injecting a `lead()` feature, which made it fail.
- Result reported as "real signal, not a usable warning".
- scikit-learn added to `requirements-ml.txt`, included in `requirements-dev.txt`.

#### Changes after an independent review of Phases 9-11 (review agent; the bot's weekly budget had run out)

- Model evaluation: the threshold is now applied to the same fitted model it was tuned on; best model and permutation importance chosen on validation, not test.
- Episodes are counted by `next_stop_event_id` (a look-ahead column used only for evaluation), and alarm runs break on a time gap over 2.5 s instead of on row numbers.
- Snowflake COPY uses `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, and the load aborts if any row has no SOURCE_FILE.
- The Snowflake role gets SELECT/INSERT/DELETE on current and future RAW tables.
- The Spark job keeps only the latest `ingest_date` copy of each source file.

## What you still need to do, in order

1. **Turn on branch protection for `main`.** On GitHub: Settings → Branches (or Rules) → add a rule for `main` → require a pull request and require the CI checks to pass. Until then GitHub does not block a red PR from being merged.
2. **Pull the result and set up your laptop** (macOS, Python 3.13):
   ```bash
   git checkout main && git pull
   python3.13 -m venv .venv && source .venv/bin/activate
   pip install -r requirements-dev.txt
   ```
   Your laptop has duckdb 2.0.1 and pandas 3.1.1, which match the pins. The venv installs everything else (dbt-core 1.12.7, dbt-duckdb 1.11.1, scikit-learn 1.9.1, pytest, ruff).
3. **Run the pipeline on the real file and check the numbers match the README.** If you still have your old `cobot.duckdb`, the loader rebuilds its legacy `raw_logs` automatically.
   ```bash
   python ingest.py
   python -m robot_pipeline.dbt_runner build
   python -m robot_pipeline.checks
   python -m robot_pipeline.report
   python -m robot_pipeline.ml.train
   pytest
   ```
4. **Try Airflow locally** (optional): follow `orchestration/README.md` (a separate venv, `airflow dags test robot_log_pipeline`, then `airflow standalone` for the UI). It was not run with the scheduler or UI here.
5. **Try the stream locally** (optional): install Docker Desktop or Java 17+, then follow `stream/README.md` (`pip install -r requirements-stream.txt`, start Kafka, run the consumer, then the producer).
6. **Webhook alerts** (optional): create an incoming webhook in Slack (or similar), then run `export ALERT_WEBHOOK_URL=...` and `export ALERT_WEBHOOK_FORMAT=slack` in your shell. Never commit the URL.
7. **Cloud (optional; needs accounts and may cost money, so check current pricing first):** follow `cloud/README.md` in this order: an AWS bucket and IAM, then `python -m robot_pipeline.cloud.s3 upload`; then Snowflake (key pair, `01_setup.sql`, the storage integration trust policy, `02_load_raw.sql`, `DBT_TARGET=snowflake ... dbt_runner build`); then Databricks (install the CLI, `databricks auth login`, `databricks bundle deploy` and `run`). Expect small fixes on first contact: none of it has run against the real services.
8. **Read `WALKTHROUGH.md`** and make sure you can answer the three questions at the end of each phase, especially Phases 4, 7 and 11. Interviewers will ask.
9. **Open questions in the data** you may want to raise or mention in interviews: what negative current and speed mean; whether `Temperature_T0` is joint 0; why the 54 unknown rows have `grip_lost = false` (and `Current_J0 = 1.0` in 8 of them); why cycle numbers 224–233 repeat.
10. **Housekeeping:**
    - Merged phase branches can be deleted on GitHub.
    - Dependabot will open weekly PRs to update the pinned GitHub Actions.
    - Sourcery's free review budget resets weekly: comment `@sourcery-ai review` on a PR to request one.
    - For this build, a personal Claude cloud environment named `robot-log-pipeline` was created that allows only `archive.ics.uci.edu`. Keep it or delete it in your Claude environment settings.
