"""Replay the recorded robot logs into Kafka, one event at a time, as if live.

THIS IS A SIMULATION. It reads the recorded UCI file and re-sends its rows; it
is not connected to a robot. To exercise the consumer, it can also inject the
problems real streams have: duplicates, out-of-order and late events.

Usage:
    python -m robot_pipeline.stream.producer                       # real time (1 row/s)
    python -m robot_pipeline.stream.producer --speed 0             # as fast as possible
    python -m robot_pipeline.stream.producer --speed 0 \\
        --duplicate-rate 0.02 --reorder-rate 0.02 --late-rate 0.01 --late-by 30
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from robot_pipeline import config
from robot_pipeline.loader import read_source
from robot_pipeline.stream.events import ROBOT_ID, TOPIC, encode, row_to_event

log = logging.getLogger(__name__)


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def load_events(path: Path) -> list[dict]:
    """Read and validate the source file, then turn each row into an event, in row order."""
    df = read_source(path)
    records = df.sort_values("Num").to_dict(orient="records")
    return [row_to_event(r, path.name) for r in records]


@dataclass
class Disorder:
    duplicates: int = 0
    reordered: int = 0
    delayed: int = 0


def simulate_disorder(
    events: list[dict],
    duplicate_rate: float = 0.0,
    reorder_rate: float = 0.0,
    late_rate: float = 0.0,
    late_by: int = 30,
    seed: int = 7,
) -> tuple[list[dict], Disorder]:
    """Return events in the order they will be sent, with injected problems.

    * late:      an event is held back and sent ``late_by`` positions later
    * reorder:   two neighbouring events swap places
    * duplicate: an event is sent again 1-5 positions later
    Deterministic for a given seed, so tests can rely on it.
    """
    rng = random.Random(seed)
    stats = Disorder()
    out: list[dict] = []
    held: list[tuple[int, dict]] = []
    for i, ev in enumerate(events):
        if late_rate and rng.random() < late_rate:
            held.append((i + late_by, ev))
            stats.delayed += 1
        else:
            out.append(ev)
        ready = [h for h in held if h[0] <= i]
        held = [h for h in held if h[0] > i]
        out.extend(ev for _, ev in ready)
    out.extend(ev for _, ev in held)

    if reorder_rate:
        i = 0
        while i < len(out) - 1:
            if rng.random() < reorder_rate:
                out[i], out[i + 1] = out[i + 1], out[i]
                stats.reordered += 1
                i += 2
            else:
                i += 1

    if duplicate_rate:
        result: list[dict] = []
        pending: list[tuple[int, dict]] = []
        for i, ev in enumerate(out):
            result.append(ev)
            if rng.random() < duplicate_rate:
                pending.append((i + rng.randint(1, 5), ev))
                stats.duplicates += 1
            due = [p for p in pending if p[0] <= i]
            pending = [p for p in pending if p[0] > i]
            result.extend(ev for _, ev in due)
        result.extend(ev for _, ev in pending)
        out = result
    return out, stats


def pace(events: Iterable[dict], speed: float, max_sleep: float):
    """Yield events, sleeping between them like the real timestamps (scaled by speed)."""
    previous: datetime | None = None
    for ev in events:
        ts = datetime.fromisoformat(ev["event_ts"])
        if speed > 0 and previous is not None and ts > previous:
            time.sleep(min((ts - previous).total_seconds() / speed, max_sleep))
        previous = ts if previous is None or ts > previous else previous
        yield ev


def ensure_topic(servers: str, topic: str = TOPIC, partitions: int = 1) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": servers})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions)])
    for fut in futures.values():
        try:
            fut.result()
        except Exception as exc:  # topic created concurrently is fine
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def produce(events: Iterable[dict], servers: str, topic: str = TOPIC) -> int:
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": servers,
            "enable.idempotence": True,  # no broker-side duplicates on retries
            "acks": "all",
            "linger.ms": 5,
        }
    )
    errors: list[str] = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(str(err))

    sent = 0
    for ev in events:
        producer.produce(topic, key=ROBOT_ID, value=encode(ev), on_delivery=on_delivery)
        producer.poll(0)
        sent += 1
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"{remaining} messages not delivered; errors: {errors[:5]}")
    return sent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Replay robot logs into Kafka (SIMULATION).")
    p.add_argument("--file", type=Path, default=config.raw_dir() / "dataset_02052023.xlsx")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--speed", type=float, default=1.0, help="1 = real time, 0 = no waiting")
    p.add_argument("--max-sleep", type=float, default=5.0, help="cap on any single wait (s)")
    p.add_argument("--limit", type=int, help="send only the first N rows")
    p.add_argument("--duplicate-rate", type=float, default=0.0)
    p.add_argument("--reorder-rate", type=float, default=0.0)
    p.add_argument("--late-rate", type=float, default=0.0)
    p.add_argument("--late-by", type=int, default=30, help="positions a late event is delayed")
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    events = load_events(args.file)[: args.limit]
    ordered, injected = simulate_disorder(
        events, args.duplicate_rate, args.reorder_rate, args.late_rate, args.late_by, args.seed
    )
    servers = bootstrap_servers()
    ensure_topic(servers, args.topic)
    log.info(
        "SIMULATED replay of %d rows from %s to %s/%s (injected: %d duplicates, "
        "%d swaps, %d delayed)",
        len(events),
        args.file.name,
        servers,
        args.topic,
        injected.duplicates,
        injected.reordered,
        injected.delayed,
    )
    sent = produce(pace(ordered, args.speed, args.max_sleep), servers, args.topic)
    log.info("Sent %d messages", sent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
