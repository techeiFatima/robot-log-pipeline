## Stream tables (`stream.duckdb`, written by the Kafka consumer; simulation)

| Table | One row per | Key columns |
|---|---|---|
| `stream_events` | event stored once | `event_id` (PK, "<file>:<Num>"), `event_ts` (naive UTC), `protective_stop`, `payload` (JSON of the 24 raw columns), `received_at`, `is_out_of_order`, `is_late`, `lateness_seconds`, `kafka_topic/partition/offset` |
| `stream_duplicates` | repeated delivery that was ignored | `event_id`, `received_at`, Kafka coordinates (unique) |
| `stream_rejects` | message that was not a valid event | `reason`, `raw_message` (first 2,000 chars), Kafka coordinates (unique) |
| `stop_alerts_sent` | protective-stop alert sent | `event_id` (PK), `robot_id`, `event_ts`, `alerted_at` |

## Files written to `state/`

| File | Content |
|---|---|
| `alerts.log` | One JSON object per alert: name, severity, message, details, dedup_key, created_at, webhook result |
| `alert_state.json` | Cooldown keys and when each was last sent (entries expire after 7 days) |
| `checks/success_check_latest.json` | The last success-test result with every count |
| `monitor_latest.json` | The last monitor run: every check's result |
| `pipeline_run_latest.json` | The last Airflow run's final status and failed task |
