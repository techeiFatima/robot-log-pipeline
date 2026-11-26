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
- **Actions pinned to full commit SHAs: checkout v7.0.1, setup-python v7.1.0, upload-artifact v7.0.2 (latest per their release pages, 2026-11-26), with Dependabot updating them weekly.** Reason: a review bot (Sourcery) flagged that tags can be moved; SHAs cannot.
- **Did not turn on branch protection / required checks.** Reason: it is a repository settings change for the owner to make; it is in FINAL_REPORT.md's to-do list, and until then merges wait for green CI by hand.
- **Added a pull request template with a review checklist.** Reason: the target role asks for code reviews; the checklist includes "no data or secrets".

## Phase 6: scheduling with Airflow

- **Airflow 3.4.0 (latest), installed with the official constraints file for Python 3.13.** Reason: Airflow's documented install method; 3.13 is supported since 3.1.
- **Ran Airflow locally with `airflow dags test` in its own virtualenv, no Docker.** Reason: Docker has no daemon in the build environment, and `dags test` runs the real tasks and records real states.
- **Each step is a `BashOperator` that calls the project's own `.venv` Python (`ROBOT_PYTHON`).** Reason: Airflow's pinned dependencies cannot clash with the pipeline's, and each step is the same command you would run by hand.
- **Three steps, `load_raw_logs >> dbt_build >> success_check`.** Reason: "load, build, test in order"; `dbt build` already runs the dbt tests, and the success check is the final gate.
- **`schedule="@daily"`, `catchup=False`, `max_active_runs=1`.** Reason: daily as asked; no backfill of past days on first start; two runs never write the same DuckDB file at once.
- **2 retries, 5 minutes apart with exponential backoff, 30-minute timeout per step.** Reason: rides out short glitches without hiding a real failure for long.
- **Retries kept even for errors that will not fix themselves (e.g. missing file).** Reason: Airflow cannot tell the two apart from an exit code; the cost is ~15 minutes before the alert.
- **`on_failure_callback` on every step; it fires only after the last retry.** Reason: one clear alert per real failure, not one per attempt.
- **`ROBOT_TASK_RETRIES` environment override.** Reason: lets a local failure test finish without waiting for retries.
- **The DAG folder is `orchestration/`, not `airflow/`.** Reason: tested, a top-level folder named `airflow` shadows the real package (`import airflow` found the folder), which made the Airflow test run where it should have been skipped.
- **A CI job installs Airflow and tests the DAG, with `REQUIRE_AIRFLOW=1` so a broken install fails instead of skipping; the job runs `airflow db migrate` first.** Reason: a skipped test shows green; the first CI run failed with "no such table: dag" because a fresh Airflow home has no metadata database (reproduced locally, then fixed).

### Changes after the code review on the Phase 6 PR (Sourcery bot)

- **An unchanged file (same name and SHA-256, rows still present) is no longer reloaded; `load_audit.action` records `loaded`, `replaced` or `unchanged`; `--force` reloads anyway.** Reason: daily re-runs were stamping old data with a new `loaded_at`, which would hide "data stopped arriving". This replaces the Phase 2 rule "always replace".
- **All files of one run load in one transaction, and every file is validated before the database is touched.** Reason: a bad second file must not leave the first one half-updated while dbt is skipped.
- **The legacy-table rebuild now happens inside that transaction.** Reason: a failed load must not lose the old table.
- **The DAG shell-quotes `ROBOT_PROJECT_DIR` and `ROBOT_PYTHON` and ignores a non-numeric `ROBOT_TASK_RETRIES`.** Reason: no shell evaluation of configured paths; a bad setting must not stop the DAG from loading.
- **DAG-level callbacks write `state/pipeline_run_latest.json` (success/failed + failed task).** Reason: when an early step fails, the last success-check report is stale and could be mistaken for the latest result.
- **Not changed: rows from a file removed from `data/raw/` stay in `raw_logs`.** Reason: raw is an archive of what arrived; deleting history because a file was moved would be the surprising behaviour. Removing a source is a deliberate manual step.
- **Switched loader queries from `fetchone()` to a full-fetch helper.** Reason: DuckDB 2 refuses a new statement while a result is only partly read (found by the new tests).

## Phase 7: real-time stream with Kafka

- **Kafka 4.4.0 in KRaft mode (no ZooKeeper), run from the official Apache tarball with Java 21 in the build environment; `apache/kafka:4.4.0` for Docker.** Reason: latest release; Docker was not available here, but Java was, so a real broker could run; the checksum was verified against Apache's `.sha512`.
- **Python client `confluent-kafka` 2.16.2, in its own `requirements-stream.txt`.** Reason: the maintained librdkafka-based client; only stream users need it.
- **The stream replays the raw rows unchanged inside `payload`, plus `event_id`, `robot_id` and a parsed `event_ts`.** Reason: the stream is the raw source in this simulation; cleaning stays in one place (dbt).
- **One topic `robot.logs.v1`, one partition, key = robot id.** Reason: Kafka only orders messages within a partition; one robot means one partition is enough, and `.v1` leaves room for a schema change.
- **Producer uses `enable.idempotence=true` and `acks=all`.** Reason: Kafka itself will not write duplicates when the producer retries; the app-level duplicates are simulated on purpose.
- **Duplicates: `event_id` primary key with `ON CONFLICT DO NOTHING`; repeats are logged in `stream_duplicates`.** Reason: stored once, still countable.
- **Late and out-of-order events are stored and flagged, never dropped; allowed lateness 10 s.** Reason: the brief forbids losing data; 10 s is ten normal row intervals.
- **Manual offset commits after the write (`enable.auto.commit=false`).** Reason: at-least-once delivery plus an idempotent store gives the same result as exactly-once, without Kafka transactions.
- **The stream writes to a separate `stream.duckdb`.** Reason: DuckDB allows one writer per file; a consumer that runs all the time would block the daily batch.
- **The consumer's idle timer starts after the first message, with a 30 s startup grace.** Reason: a test failed once because a fresh broker's group coordinator took longer than the 5 s idle timeout (seen in testing, then fixed).
- **CI runs the broker test against an `apache/kafka:4.4.0` service container, with `REQUIRE_KAFKA=1` so it cannot silently skip.** Reason: that checks the Docker image path on GitHub, which this build environment could not run.
- **Disorder injection is deterministic (seeded).** Reason: tests can assert exact counts.

### Changes after the code review on the Phase 7 PR (Sourcery bot)

- **Offsets are committed explicitly, only up to the last fully handled message.** Reason: the bot found that a plain `commit()` after an exception could commit past the failing message, so it was skipped on restart; a new broker test reproduced this on the old code (message 50 skipped) and passes on the fix.
- **Only fatal Kafka errors stop the consumer; others are logged and polling continues.** Reason: librdkafka recovers from transient broker errors by itself.
- **`stream_duplicates` and `stream_rejects` are unique per (topic, partition, offset).** Reason: a redelivery after a crash must not count as a second duplicate or reject.
- **All event times are normalised to naive UTC; `decode` rejects a non-object `payload`.** Reason: mixing aware and naive times would crash the comparison, and a bad payload must go to `stream_rejects`, not stop the consumer.
- **The package config now includes `robot_pipeline.stream`** (`packages.find`, checked by building a wheel). Reason: a built wheel was missing the stream modules.
- **The Docker Compose broker binds to `127.0.0.1` only.** Reason: it has no authentication or TLS.
- **`REQUIRE_KAFKA=1` now also fails if the client library cannot be imported.** Reason: an import error must not turn into a skip.
- **Not changed: `event_id` stays `<file name>:<Num>`.** Reason: it is the same key as the batch `log_id`, and replaying a corrected file under the same name should not double-count events; the batch path is where corrected files replace data.
- **Not changed: one consumer per `stream.duckdb` file.** Reason: DuckDB allows one writer per file and refuses a second process at start-up with a lock error; documented in `stream/README.md`. A second consumer group needs its own `--db`.
## Phase 8: monitoring and alerting

- **Alerts always go to `state/alerts.log` (JSON lines); `ALERT_WEBHOOK_URL` adds an HTTP POST; `ALERT_WEBHOOK_FORMAT=slack` sends Slack's `{"text": ...}` shape.** Reason: the brief's default and option; JSON lines are easy to search and parse.
- **`alerts.py` uses only the standard library, and a failing webhook is logged, never raised.** Reason: it runs from Airflow callbacks too, and alerting must never crash the pipeline it watches.
- **Batch alerts repeat at most once per day per alert (cooldown keyed by name and date).** Reason: a daily stale-data condition should not page someone on every re-run.
- **"Data stopped arriving" uses the last load with new or changed content, threshold 26 h.** Reason: a daily schedule plus 2 hours of slack; unchanged re-runs must not reset the clock (the Phase 6 review fix).
- **"Row counts far below normal" is checked two ways: latest load vs the median of the last 10 loads (needs 3), and each hour vs the median hour; threshold 50 %.** Reason: with one file there is no load history yet, but hourly volume can be judged today; the median resists outliers.
- **"Failed checks" reads the success-check report, dbt's `run_results.json` and the pipeline run status.** Reason: covers the custom success test, every dbt test, and a run that failed before the checks ran.
- **The business alert lives in the stream consumer, not the daily batch.** Reason: a daily batch cannot alert within 5 minutes; the consumer sees each event within seconds.
- **One business alert per stop episode: a stop with no other stop from the same robot in the previous 5 s of event time; an optional cooldown is off by default.** Reason: matches the batch gap rule without needing row order across out-of-order arrivals; nothing is suppressed unless asked.
- **The alert records `seconds_from_sent_to_alert`, measured from the producer's `sent_at`.** Reason: the replayed `event_ts` is from 2022, so "within 5 minutes" is measured from when the simulated robot sent the event; the target is checked on every alert, not assumed.
- **An out-of-order stop that arrives before its predecessor can raise one extra alert.** Reason: erring toward alerting is safer than missing a stop.
- **The consumer now treats "unknown topic" as "wait", not a crash.** Reason: found in testing; a consumer started before the producer must not die.
- **Airflow: a `monitor` task (`trigger_rule=all_done`) runs after the checks even when a step failed; an empty `pipeline_ok` task is the only leaf.** Reason: Airflow sets a run's final state from its leaf tasks; without `pipeline_ok`, a successful monitor would have marked failed runs green.
- **Task failure callbacks send `pipeline_task_failed` through `robot_pipeline.alerts` (standard library only, imported from the project folder).** Reason: Airflow's environment needs none of the pipeline's dependencies.

### Changes after the code review on the Phase 8 PR (Sourcery bot, 22 comments)

- **Sent stop alerts are recorded in `stop_alerts_sent` in the stream store; a redelivered stop without a recorded alert is alerted.** Reason: a crash between storing and alerting would otherwise lose the alert, because the redelivery is a "duplicate".
- **The stop cooldown is read from that table, so it survives restarts; tied timestamps count as one episode.** Reason: review findings; both have tests.
- **A malformed `sent_at` no longer blocks the alert (the delay is then unknown).** Reason: losing a stop alert over a bad timing field is the wrong failure.
- **Alert cooldown state: an exclusive file lock (`fcntl`, macOS/Linux) around check-and-record, entries expire after 7 days, the cooldown only starts when delivery worked, and stop alerts no longer use it.** Reason: concurrent senders, unbounded growth, and a failed webhook suppressing retries.
- **Webhook: no redirects followed; a malformed URL is logged as a failed delivery; control characters stripped from the logged text.** Reason: a redirect could replay alert data elsewhere; alerting must never crash; no forged log lines.
- **Monitor: the batch cooldown is 24 h (once per day); thresholds must be finite and positive; files of one loader run are compared as one batch; hours with zero rows count as low; duplicates and rejects count as stream activity; a missing `run_results.json` is a problem; inside Airflow the monitor ignores the previous run's status file (`--ignore-run-status`).** Reason: each was a real way to get a false OK or a false alarm.
- **A forced reload of unchanged content is recorded as `reloaded` and does not count as new data.** Reason: `--force` must not hide "data stopped arriving".
- **Not changed: a durable webhook outbox.** Reason: delivery stays at-least-once; a rare duplicate alert after a crash is acceptable, a missed one is not.
- **Not changed: the episode lookup query.** Reason: it only reads a 5-second window per stop; measured 0.016 s median delay end to end.
- **The DAG failure callback now imports `robot_pipeline.alerts` directly instead of running it as a subprocess.** Reason: opengrep flagged the subprocess call. It was not injectable (argument list, no shell), but `alerts.py` is standard-library only, so a direct import is simpler and removes the question entirely.

## Phase 9: cloud (S3, Snowflake, Databricks): NOT RUN, needs accounts

- **No accounts created, no credentials entered, nothing spent; everything is marked "NOT RUN, NEEDS ACCOUNTS".** Reason: hard limit in the brief.
- **S3 layout `raw/robot_logs/ingest_date=YYYY-MM-DD/` (original `.xlsx`, byte-for-byte) and `staged/robot_logs/...` (CSV of the same rows plus `source_file`).** Reason: keep the untouched original; Snowflake's COPY cannot read Excel; date partitions make reloads and lifecycle rules simple.
- **Files are validated with the loader's rules before upload; re-upload of identical content is skipped (SHA-256 in object metadata).** Reason: bad files never reach the bucket; daily re-runs cost nothing.
- **Credentials only through boto3's default chain (SSO profile or IAM role); least-privilege policies limited to the two prefixes.** Reason: no secret ever needs to be in the repo or the environment of the code.
- **S3 code tested with `moto` (an in-memory S3 fake), locally and in a CI job.** Reason: verifies the real boto3 calls without an account.
- **Snowflake: key-pair auth for a `TYPE = SERVICE` user, a role scoped to the two schemas, XSMALL warehouse with 60 s auto-suspend.** Reason: dbt's docs note Snowflake is deprecating single-factor passwords; the smallest, self-pausing warehouse keeps cost minimal.
- **Snowflake raw table uses the exact quoted source column names (including `"cycle "`), so the same dbt models run on both targets unchanged.** Reason: one set of models; the success test runs on Snowflake as dbt tests.
- **Only one engine-specific macro: `parse_iso_utc_ts` (dispatch), with an explicit format on Snowflake.** Reason: avoids relying on Snowflake's AUTO timestamp detection.
- **The Snowflake load copies into a temporary table, then deletes and inserts by `source_file` in one transaction.** Reason: the same no-duplicates rule as the local loader; COPY's own load history alone would let a corrected file under a new path duplicate rows.
- **Snowflake verified only by `dbt parse` on the Snowflake target and by parsing all 37 compiled SQL files with sqlglot's Snowflake dialect.** Reason: `dbt compile` connects to Snowflake even with `--no-introspect` (tested); anything stronger needs an account.
- **Databricks transformation = the staging cleanup plus the success test in PySpark, which fails the job before writing if the test fails.** Reason: "one transformation", and the most important one to keep identical across engines.
- **Deployed as a Declarative Automation Bundle (formerly Asset Bundles) with one serverless Python task.** Reason: the current, code-first way to define Databricks jobs from a repo.
- **The PySpark code was run on local Apache Spark 4.3.0 on the real file and compared with dbt's output: identical on all 30 shared columns.** Reason: proves the logic without a workspace; Databricks itself is not verified.
- **PySpark is not in any requirements file for the main venv; CI installs it in its own job.** Reason: it is large and only needed for this one check; Databricks provides it at run time.
