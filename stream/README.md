# Real-time stream (Kafka): a simulation

> **This is a replay, not a live robot.** The producer reads the recorded
> UCI file and re-sends its rows one at a time, waiting between them like
> the original timestamps, as if the robot were sending them now. Nothing
> here is connected to a real robot.

```
recorded file ──> producer ──> Kafka topic robot.logs.v1 ──> consumer ──> stream.duckdb
  (UCI xlsx)     (replay +      (1 partition, key =         (dedup, late,    stream_events
                  optional       ur3-cobot-01)               out-of-order)    stream_duplicates
                  disorder)                                                   stream_rejects
```

## What each event looks like

One JSON message per log row:

```json
{"schema_version": 1, "event_id": "dataset_02052023.xlsx:42", "robot_id": "ur3-cobot-01",
 "source_file": "dataset_02052023.xlsx", "row_num": 42,
 "event_ts": "2022-10-26T08:18:03.067", "payload": {"Num": 42, "Timestamp": "...", "...": "..."},
 "sent_at": "2026-11-26T21:35:00+00:00"}
```

`payload` holds the 24 source columns exactly as delivered. `event_id`
is the deduplication key. `robot_id` is a made-up name for the one robot
in the dataset.

## How messy delivery is handled

| Problem | What the consumer does |
|---|---|
| **Same event twice** | Stored once (`event_id` is the primary key). The repeat is logged in `stream_duplicates` so it can be counted. |
| **Out of order** | Stored and flagged `is_out_of_order`. Analysis orders by `event_ts`, never by arrival. |
| **Late** (older than the newest event by more than `--allowed-lateness`, default 10 s) | **Kept**, flagged `is_late`, with `lateness_seconds`. Never dropped. |
| **Not a valid event** | Saved in `stream_rejects` with the reason; the consumer keeps going. |
| **Consumer crash or restart** | Offsets are committed only after the write, so a few messages may come again; the dedup makes that harmless (at-least-once delivery + idempotent store). The lateness watermark is reloaded from the table. |

| **A message fails while being handled** | Its offset is never committed, so it is read again after a restart (tested). |
| **Transient broker error** | Logged; the client reconnects by itself. Only fatal errors stop the consumer. |

**One consumer per `stream.duckdb`.** DuckDB allows one writing process
per file; a second consumer on the same file stops at start-up with a
lock error. Give each consumer group its own `--db`.

The producer can inject these problems on purpose:
`--duplicate-rate`, `--reorder-rate`, `--late-rate`, `--late-by`
(deterministic for a given `--seed`).

## Run it on your laptop

Install the Kafka client into the project venv:

```bash
pip install -r requirements-stream.txt
```

Start a single-node Kafka. **Option A, Docker Desktop** (the port is
bound to `127.0.0.1` because this broker has no authentication):

```bash
docker compose -f stream/docker-compose.yml up -d
```

**Option B, no Docker** (needs Java 17+, for example `brew install openjdk@21`):

```bash
curl -LO https://archive.apache.org/dist/kafka/4.4.0/kafka_2.13-4.4.0.tgz
tar -xzf kafka_2.13-4.4.0.tgz && cd kafka_2.13-4.4.0
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

In one terminal, start the consumer:

```bash
python -m robot_pipeline.stream.consumer
```

In another, replay the file in real time (about 2 hours for 7,409 rows;
gaps are capped at 5 s):

```bash
python -m robot_pipeline.stream.producer
```

Or replay it as fast as possible, with injected problems:

```bash
python -m robot_pipeline.stream.producer --speed 0 --duplicate-rate 0.02 --reorder-rate 0.02 --late-rate 0.01 --late-by 30
```

Stop Docker Kafka afterwards with `docker compose -f stream/docker-compose.yml down`.
