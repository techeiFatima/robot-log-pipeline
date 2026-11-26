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
    """Read and store messages. ``idle_timeout`` counts only after the first
    message; before that, ``startup_timeout`` allows for group coordination on a
    freshly started broker (which can take several seconds)."""
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after the write succeeded
        }
    )
    consumer.subscribe([topic])
    handled = 0
    uncommitted = 0
    started = time.monotonic()
    last_message: float | None = None
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
            if msg.error():
                raise KafkaException(msg.error())
            last_message = time.monotonic()
            try:
                event = decode(msg.value())
            except BadEvent as exc:
                store.reject(str(exc), msg.value(), msg.partition(), msg.offset())
            else:
                outcome = store.handle(event, msg.partition(), msg.offset())
                for hook in hooks or []:
                    hook(event, outcome)
            handled += 1
            uncommitted += 1
            if uncommitted >= commit_every:
                consumer.commit(asynchronous=False)
                uncommitted = 0
    finally:
        if uncommitted:
            consumer.commit(asynchronous=False)
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
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    store = StreamStore(args.db, args.allowed_lateness)
    try:
        n = consume(store, servers, args.topic, args.group, args.idle_timeout, args.max_messages)
        log.info("Handled %d messages this run: %s", n, store.stats.counts)
        log.info("Totals in %s: %s", args.db, store.summary())
    except KeyboardInterrupt:
        log.info("Stopped. Totals: %s", store.summary())
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
