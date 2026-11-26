"""Store stream events in DuckDB, handling duplicates, late and out-of-order events.

Rules (all independent of Kafka, so they are unit-tested without a broker):

* **Twice:** an event_id is stored once. A repeat is not stored again; it is
  recorded in stream_duplicates so it can be counted.
* **Out of order:** an event older than the newest event already seen (the
  watermark) is stored normally and flagged is_out_of_order.
* **Late:** an event older than the watermark by more than allowed_lateness
  seconds is still stored (never dropped), flagged is_late, and counted.
* Analysis always orders by event_ts, never by arrival, so order of arrival
  does not change results.
* **Not an event:** a message that cannot be parsed goes to stream_rejects.

The watermark is reloaded from the table on start, so a restarted consumer
keeps judging lateness correctly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import duckdb

DDL = """
CREATE TABLE IF NOT EXISTS stream_events (
    event_id          VARCHAR PRIMARY KEY,
    robot_id          VARCHAR NOT NULL,
    source_file       VARCHAR NOT NULL,
    row_num           BIGINT NOT NULL,
    event_ts          TIMESTAMP NOT NULL,
    protective_stop   DOUBLE,
    payload           JSON NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL,
    arrival_seq       BIGINT NOT NULL,
    is_out_of_order   BOOLEAN NOT NULL,
    is_late           BOOLEAN NOT NULL,
    lateness_seconds  DOUBLE NOT NULL,
    kafka_partition   INTEGER,
    kafka_offset      BIGINT
);
CREATE TABLE IF NOT EXISTS stream_duplicates (
    event_id        VARCHAR NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER,
    kafka_offset    BIGINT
);
CREATE TABLE IF NOT EXISTS stream_rejects (
    received_at     TIMESTAMPTZ NOT NULL,
    reason          VARCHAR NOT NULL,
    raw_message     VARCHAR,
    kafka_partition INTEGER,
    kafka_offset    BIGINT
);
"""


class Outcome(StrEnum):
    STORED = "stored"
    OUT_OF_ORDER = "out_of_order"
    LATE = "late"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass
class StoreStats:
    counts: dict[str, int] = field(default_factory=lambda: {o.value: 0 for o in Outcome})

    def add(self, outcome: Outcome) -> None:
        self.counts[outcome.value] += 1


class StreamStore:
    def __init__(self, db_path: Path | str, allowed_lateness_seconds: float = 10.0):
        self.con = duckdb.connect(str(db_path))
        self.con.execute(DDL)
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.stats = StoreStats()
        wm, seq = self.con.execute(
            "SELECT max(event_ts), coalesce(max(arrival_seq), 0) FROM stream_events"
        ).fetchone()
        self.watermark: datetime | None = wm
        self.arrival_seq: int = int(seq)

    def close(self) -> None:
        self.con.close()

    def reject(self, reason: str, raw: bytes | None, partition=None, offset=None) -> Outcome:
        text = raw.decode("utf-8", "replace")[:2000] if raw is not None else None
        self.con.execute(
            "INSERT INTO stream_rejects VALUES (?, ?, ?, ?, ?)",
            [datetime.now(UTC), reason, text, partition, offset],
        )
        self.stats.add(Outcome.REJECTED)
        return Outcome.REJECTED

    def handle(
        self,
        event: dict[str, Any],
        partition: int | None = None,
        offset: int | None = None,
        received_at: datetime | None = None,
    ) -> Outcome:
        """Store one decoded event and say what kind of arrival it was."""
        received_at = received_at or datetime.now(UTC)
        ts: datetime = event["event_ts_parsed"]
        lateness = (self.watermark - ts) if self.watermark and ts < self.watermark else timedelta(0)
        out_of_order = lateness > timedelta(0)
        late = lateness > self.allowed_lateness
        stop = event["payload"].get("Robot_ProtectiveStop")

        inserted = self.con.execute(
            """INSERT INTO stream_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
            [
                event["event_id"],
                event["robot_id"],
                event["source_file"],
                int(event["row_num"]),
                ts,
                stop,
                json.dumps(event["payload"]),
                received_at,
                self.arrival_seq + 1,
                out_of_order,
                late,
                lateness.total_seconds(),
                partition,
                offset,
            ],
        ).fetchall()
        if not inserted:
            self.con.execute(
                "INSERT INTO stream_duplicates VALUES (?, ?, ?, ?)",
                [event["event_id"], received_at, partition, offset],
            )
            self.stats.add(Outcome.DUPLICATE)
            return Outcome.DUPLICATE

        self.arrival_seq += 1
        if self.watermark is None or ts > self.watermark:
            self.watermark = ts
        outcome = Outcome.LATE if late else Outcome.OUT_OF_ORDER if out_of_order else Outcome.STORED
        self.stats.add(outcome)
        return outcome

    def summary(self) -> dict[str, int]:
        """Counts from the tables (not memory), so they survive restarts."""
        q = """
        SELECT
          (SELECT count(*) FROM stream_events)                              AS events_stored,
          (SELECT count(*) FROM stream_events WHERE is_out_of_order)        AS out_of_order,
          (SELECT count(*) FROM stream_events WHERE is_late)                AS late,
          (SELECT count(*) FROM stream_duplicates)                          AS duplicates_ignored,
          (SELECT count(*) FROM stream_rejects)                             AS rejected,
          (SELECT count(*) FROM stream_events WHERE protective_stop = 1)    AS stop_events,
          (SELECT count(*) FROM stream_events WHERE protective_stop IS NULL) AS unknown_status
        """
        cur = self.con.execute(q)
        names = [d[0] for d in cur.description]
        return dict(zip(names, (int(v) for v in cur.fetchone()), strict=True))
