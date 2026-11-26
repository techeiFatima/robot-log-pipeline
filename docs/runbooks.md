# Alert runbooks

One section per alert. Each says what fired it, what it usually means,
how to check it, and how to fix it. Alerts are written to
`state/alerts.log` (one JSON line each) and, if `ALERT_WEBHOOK_URL` is
set, POSTed to that webhook (`ALERT_WEBHOOK_FORMAT=slack` for Slack's
format).

Quick look at recent alerts:

```bash
tail -n 20 state/alerts.log | python -m json.tool --json-lines
```

Batch alerts repeat at most once per day per alert name (cooldown). The
protective-stop alert fires once per stop episode.

---

## `protective_stop` (critical, business alert)

**Fires when:** the stream consumer stores a stop row
(`Robot_ProtectiveStop = 1`) and the same robot had no other stop in the
previous 5 seconds of event time, i.e. a new stop episode begins.
Measured delay from the event being sent to the alert: median 0.016 s,
max 2.2 s in a 1,500-row concurrent test run (target: 5 minutes).

**Means:** the robot stopped itself for safety (for example a collision
or overload was detected). In this project the "robot" is a replay of
recorded data.

**Check:**
- `details.event_ts` and `details.grip_lost` in the alert; the message names the cycle.
- What it was doing just before:
  ```sql
  -- in stream.duckdb
  SELECT event_ts, protective_stop, payload->>'Tool_current' AS tool_current
  FROM stream_events WHERE event_ts BETWEEN TIMESTAMP '<event_ts>' - INTERVAL 30 SECOND
                                         AND TIMESTAMP '<event_ts>' + INTERVAL 30 SECOND
  ORDER BY event_ts;
  ```
- `details.arrival = "late"` means the event came in late; the stop
  happened earlier than the alert suggests.

**Fix / act:** this alert is for the operations team, not a pipeline
fault. Inspect the cell, clear the cause, restart the robot. If stops come
in bursts and alerts are too noisy, start the consumer with
`--stop-alert-cooldown 300`, which limits it to one alert per 5 minutes of
event time per robot.

---

## `data_stopped_arriving` (critical)

**Fires when:** no load brought **new or changed** data for more than
`ROBOT_MAX_HOURS_WITHOUT_DATA` hours (default 26). Re-loading an unchanged
file does not count (`load_audit.action = 'unchanged'`).

**Means:** the upstream export stopped, the file landed in the wrong
folder, or the same file is being delivered again and again.

**Check:**
```sql
SELECT source_file, action, rows_loaded, loaded_at FROM load_audit ORDER BY loaded_at DESC LIMIT 10;
```
```bash
ls -la data/raw/
```

**Fix:** find out why the source stopped sending; put the new file in
`data/raw/` (or `ROBOT_RAW_DIR`); re-run `python ingest.py`. For this
portfolio project there is only one file, so this alert is **expected to
fire after day one**.

---

## `row_count_low` (critical)

**Fires when:** the latest load has fewer than `ROBOT_LOW_VOLUME_RATIO`
(default 50 %) of the median rows of the previous (up to 10) loads. Needs
at least 3 earlier loads; with fewer, it reports "not enough history" and
does not alert.

**Means:** a truncated export, a robot that ran only part of the day, or
a filter upstream.

**Check:** `SELECT * FROM load_audit ORDER BY loaded_at DESC;` and compare
`rows_loaded`. Open the file and check its first and last timestamps.

**Fix:** if the file is truncated, ask for a re-send. A corrected file
under the same name replaces the old rows on the next `python ingest.py`.

---

## `hourly_rows_low` (warning)

**Fires when:** an hour in `agg_stops_by_hour` has fewer than 50 % of
the median rows per hour.

**Means:** the robot was idle, or the logger stopped for part of that
hour. On the UCI file this fires for **09:00 UTC (375 rows against a
median of 2,270)**: recording stopped about 6 minutes into that hour, and
there is a 5.2-hour gap before 14:19.

**Check:**
```sql
SELECT hour_utc, rows_total, minutes_recorded FROM agg_stops_by_hour ORDER BY 1;
SELECT row_num, event_ts, seconds_since_previous FROM int_robot_log_sequence WHERE is_after_gap;
```

**Fix:** usually nothing to fix in the pipeline. Confirm with the robot's
owners whether it was really idle. Treat stop rates for that hour with
care (small sample).

---

## `check_failed` (critical)

**Fires when:** any of these is true:
- `state/checks/success_check_latest.json` says `passed: false` (a source
  stop is missing from the clean table, a stop was invented, or the
  unknown rows differ);
- `dbt/target/run_results.json` has a test or model with status `fail`
  or `error`;
- `state/pipeline_run_latest.json` says the last Airflow run failed.

**Check:**
```bash
python -m robot_pipeline.checks
python -m robot_pipeline.dbt_runner build
```
The alert's `details.dbt_failures` names the failing dbt nodes. To see
the rows a dbt test found, run `python -m robot_pipeline.dbt_runner test
--select <test_name> --store-failures`.

**Fix:** never "fix" by relaxing the test. Typical causes: a new source
value (for example a stop value of 2) shows up as `stop_status =
'invalid'` → ask what it means, then extend the mapping in
`stg_robot_logs.sql`; or a model change dropped rows → revert or fix the
SQL. Re-run the pipeline afterwards.

---

## `pipeline_task_failed` (critical)

**Fires when:** an Airflow step fails after its retries (sent by the
DAG's `on_failure_callback`).

**Check:** the message names the task and the run. In the Airflow UI,
open the run and the first red task's log. Common messages:
- `Source file not found` / `No .xlsx files` → see `data_stopped_arriving`.
- `The source file's columns changed` → the export format changed. Compare
  the header with `EXPECTED_COLUMNS` in `robot_pipeline/loader.py`.
- `Load failed and was rolled back, nothing changed` → a value could not
  be stored as its column's type. The raw table is untouched.

**Fix:** fix the cause, then clear the failed task in the Airflow UI (or
re-run `airflow dags test robot_log_pipeline`).

---

## `stream_silent` (warning)

**Fires when:** `python -m robot_pipeline.monitor --stream` finds no event
received by the consumer for more than `ROBOT_MAX_STREAM_SILENCE_MINUTES`
(default 5).

**Means:** the producer stopped, the broker is down, or the consumer
crashed. Without the stream, **`protective_stop` alerts cannot fire**, so
this alert guards the business alert.

**Check:**
```bash
docker ps                         # is Kafka running? (Docker setup)
ps aux | grep robot_pipeline.stream
```

**Fix:** restart the broker, then the consumer
(`python -m robot_pipeline.stream.consumer`). It resumes from its last
committed offset, and duplicates are ignored. In this simulation, the
alert also fires normally once a replay has finished.
