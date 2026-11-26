"""Consume robot log events from Kafka and store them in DuckDB.

Delivery is at-least-once: offsets are committed only after the events are
written, so a crash can re-deliver a few messages. The store ignores repeats
by event_id, so the result is the same as exactly-once.

Usage:
    python -m robot_pipeline.stream.consumer                  # run until Ctrl+C
    python -m robot_pipeline.stream.consumer --idle-timeout 10  # stop after 10 s of silence
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path

from robot_pipeline import config
from robot_pipeline.stream.events import TOPIC, BadEvent, decode
from robot_pipeline.stream.store import Outcome, StreamStore

log = logging.getLogger(__name__)

EventHook = Callable[[dict, Outcome], None]


def stream_db_path() -> Path:
    value = os.environ.get("ROBOT_STREAM_DB_PATH")
    return Path(value).expanduser().resolve() if value else config.PROJECT_ROOT / "stream.duckdb"


def consume(
    store: StreamStore,
    servers: str,
    topic: str = TOPIC,
    group_id: str = "robot-log-store",
    idle_timeout: float | None = None,
    max_messages: int | None = None,
    commit_every: int = 100,
    hooks: list[EventHook] | None = None,
    startup_timeout: float = 30.0,
) -> int:
    """Read and store messages.

    Offsets are committed explicitly, and only up to the last message that was
    fully handled (stored, counted as a duplicate, or rejected). If handling a
    message raises, its offset is never committed, so it is read again after a
    restart instead of being skipped.

    ``idle_timeout`` counts only after the first message; before that,
    ``startup_timeout`` allows for group coordination on a fresh broker.
    """
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after the write succeeded
        }
    )
    consumer.subscribe([topic])
    # Errors that mean "nothing to read yet", not "broken": e.g. the consumer
    # started before the producer created the topic.
    waiting = {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF}
    done: dict[tuple[str, int], int] = {}  # (topic, partition) -> last handled offset
    handled = 0
    uncommitted = 0
    started = time.monotonic()
    last_message: float | None = None

    def commit_done() -> None:
        if done:
            offsets = [TopicPartition(t, p, o + 1) for (t, p), o in done.items()]
            consumer.commit(offsets=offsets, asynchronous=False)

    try:
        while max_messages is None or handled < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                now = time.monotonic()
                if idle_timeout is not None:
                    if last_message is None and now - started > max(startup_timeout, idle_timeout):
                        break
                    if last_message is not None and now - last_message > idle_timeout:
                        break
                continue
            err = msg.error()
            if err:
                if err.code() in waiting:
                    log.info("Waiting: %s", err.str())
                    continue
                if err.fatal():
                    raise KafkaException(err)
                # Non-fatal broker/network errors: librdkafka recovers by itself.
                log.warning("Kafka error (non-fatal, continuing): %s", err.str())
                time.sleep(1)
                continue
            last_message = time.monotonic()
            where = (msg.topic(), msg.partition(), msg.offset())
            try:
                event = decode(msg.value())
            except BadEvent as exc:
                store.reject(str(exc), msg.value(), *where)
            else:
                outcome = store.handle(event, *where)
                for hook in hooks or []:
                    hook(event, outcome)
            done[(msg.topic(), msg.partition())] = msg.offset()
            handled += 1
            uncommitted += 1
            if uncommitted >= commit_every:
                commit_done()
                uncommitted = 0
    finally:
        if uncommitted:
            commit_done()
        consumer.close()
    return handled


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Store robot log events from Kafka in DuckDB.")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--group", default="robot-log-store")
    p.add_argument("--db", type=Path, default=stream_db_path())
    p.add_argument("--allowed-lateness", type=float, default=10.0, help="seconds")
    p.add_argument("--idle-timeout", type=float, help="stop after this many silent seconds")
    p.add_argument("--max-messages", type=int)
    p.add_argument("--no-stop-alerts", action="store_true", help="disable the business alert")
    p.add_argument("--stop-alert-cooldown", type=float, default=0.0, help="seconds of event time")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    store = StreamStore(args.db, args.allowed_lateness)
    from robot_pipeline.stream.stop_alerts import StopAlerter

    alerter = (
        None
        if args.no_stop_alerts
        else StopAlerter(store, cooldown_seconds=args.stop_alert_cooldown)
    )
    try:
        n = consume(
            store,
            servers,
            args.topic,
            args.group,
            args.idle_timeout,
            args.max_messages,
            hooks=[alerter] if alerter else None,
        )
        log.info("Handled %d messages this run: %s", n, store.stats.counts)
        if alerter:
            log.info(
                "Protective-stop alerts sent: %d (max delay from send to alert: %.2f s)",
                alerter.alerts_sent,
                alerter.max_delay_seconds,
            )
        log.info("Totals in %s: %s", args.db, store.summary())
    except KeyboardInterrupt:
        log.info("Stopped. Totals: %s", store.summary())
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
