"""Business alert: tell people within 5 minutes when the robot stops itself.

Runs inside the Kafka consumer, on every stored event, so the delay is the
time from the robot sending the event to the consumer reading it (seconds),
far inside the 5-minute target. Each alert records that delay.

One alert per stop *episode*, not per row: a stop row starts a new episode
when the same robot has no other stop row in the previous
``episode_gap_seconds`` of event time (default 5 s, matching the batch
models' gap rule). ``cooldown_seconds`` (event time, default 0 = off) can
further limit alerts when stops come in bursts.

The replay is a simulation, so event_ts is the 2022 recording time; the
delay is measured from sent_at (when the replay "robot" sent the event).

Sent alerts are recorded in the stream store (table stop_alerts_sent), in
the same database as the events. So if the consumer stops after storing a
stop but before alerting, the redelivered message (now a "duplicate") still
gets its alert, and the cooldown survives restarts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from robot_pipeline.alerts import Alert, send
from robot_pipeline.stream.store import Outcome, StreamStore

TARGET_SECONDS = 300  # the business requirement: alert within 5 minutes


SENT_DDL = """
CREATE TABLE IF NOT EXISTS stop_alerts_sent (
    event_id   VARCHAR PRIMARY KEY,
    robot_id   VARCHAR NOT NULL,
    event_ts   TIMESTAMP NOT NULL,
    alerted_at TIMESTAMPTZ NOT NULL
)
"""


def _sent_at_delay(sent_at, now: datetime) -> float | None:
    """Seconds since the producer sent the event; None if sent_at is unusable."""
    if not isinstance(sent_at, str):
        return None
    try:
        ts = datetime.fromisoformat(sent_at)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return (now - ts).total_seconds()


class StopAlerter:
    def __init__(
        self,
        store: StreamStore,
        episode_gap_seconds: float = 5.0,
        cooldown_seconds: float = 0.0,
    ):
        self.store = store
        self.store.con.execute(SENT_DDL)
        self.episode_gap = timedelta(seconds=episode_gap_seconds)
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.alerts_sent = 0
        self.max_delay_seconds = 0.0

    def _q(self, sql: str, params: list):
        return self.store.con.execute(sql, params).fetchall()

    def _already_alerted(self, event_id: str) -> bool:
        return bool(self._q("SELECT 1 FROM stop_alerts_sent WHERE event_id = ?", [event_id]))

    def _is_episode_start(self, event: dict) -> bool:
        """No other stop from this robot in the previous gap, *including* stops
        with exactly the same timestamp (so tied rows are one episode). The
        query only looks at that short time window."""
        ts = event["event_ts_parsed"]
        rows = self._q(
            """SELECT count(*) FROM stream_events
               WHERE robot_id = ? AND protective_stop = 1 AND event_id <> ?
                 AND event_ts >= ? AND event_ts <= ?""",
            [event["robot_id"], event["event_id"], ts - self.episode_gap, ts],
        )
        return rows[0][0] == 0

    def _in_cooldown(self, robot: str, ts: datetime) -> bool:
        if not self.cooldown:
            return False
        rows = self._q(
            "SELECT max(event_ts) FROM stop_alerts_sent WHERE robot_id = ? AND event_ts <= ?",
            [robot, ts],
        )
        last = rows[0][0]
        return last is not None and ts - last < self.cooldown

    def __call__(self, event: dict, outcome: Outcome) -> None:
        if outcome == Outcome.REJECTED:
            return
        if event["payload"].get("Robot_ProtectiveStop") != 1:
            return
        # A duplicate is only skipped if its alert really went out; otherwise
        # this is the redelivery after a crash and the alert is retried.
        if self._already_alerted(event["event_id"]):
            return
        if not self._is_episode_start(event):
            return
        robot, ts = event["robot_id"], event["event_ts_parsed"]
        if self._in_cooldown(robot, ts):
            return

        now = datetime.now(UTC)
        sent_at = event.get("sent_at")
        delay = _sent_at_delay(sent_at, now)
        late_note = " (arrived late)" if outcome == Outcome.LATE else ""
        alert = Alert(
            name="protective_stop",
            severity="critical",
            message=(
                f"Robot {robot} stopped itself (protective stop) at {ts.isoformat()} UTC"
                f"{late_note}. Cycle {event['payload'].get('cycle ')}."
            ),
            details={
                "robot_id": robot,
                "event_id": event["event_id"],
                "event_ts": ts.isoformat(),
                "sent_at": sent_at,
                "alerted_at": now.isoformat(),
                "seconds_from_sent_to_alert": delay,
                "within_target": delay is not None and delay <= TARGET_SECONDS,
                "arrival": outcome.value,
                "grip_lost": event["payload"].get("grip_lost"),
                "simulation": True,
            },
        )
        # No dedup_key: repeats are prevented by stop_alerts_sent, so the shared
        # cooldown state file does not grow with one key per stop.
        send(alert)
        self.store.con.execute(
            "INSERT INTO stop_alerts_sent VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            [event["event_id"], robot, ts, now],
        )
        self.alerts_sent += 1
        if delay is not None:
            self.max_delay_seconds = max(self.max_delay_seconds, delay)
