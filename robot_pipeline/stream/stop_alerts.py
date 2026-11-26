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
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from robot_pipeline.alerts import Alert, send
from robot_pipeline.stream.store import Outcome, StreamStore

TARGET_SECONDS = 300  # the business requirement: alert within 5 minutes


class StopAlerter:
    def __init__(
        self,
        store: StreamStore,
        episode_gap_seconds: float = 5.0,
        cooldown_seconds: float = 0.0,
    ):
        self.store = store
        self.episode_gap = timedelta(seconds=episode_gap_seconds)
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.last_alert_event_ts: dict[str, datetime] = {}
        self.alerts_sent = 0
        self.max_delay_seconds = 0.0

    def _is_episode_start(self, event: dict) -> bool:
        ts = event["event_ts_parsed"]
        rows = self.store.con.execute(
            """SELECT count(*) FROM stream_events
               WHERE robot_id = ? AND protective_stop = 1 AND event_id <> ?
                 AND event_ts >= ? AND event_ts < ?""",
            [event["robot_id"], event["event_id"], ts - self.episode_gap, ts],
        ).fetchall()
        return rows[0][0] == 0

    def __call__(self, event: dict, outcome: Outcome) -> None:
        if outcome in (Outcome.DUPLICATE, Outcome.REJECTED):
            return
        if event["payload"].get("Robot_ProtectiveStop") != 1:
            return
        if not self._is_episode_start(event):
            return
        robot, ts = event["robot_id"], event["event_ts_parsed"]
        last = self.last_alert_event_ts.get(robot)
        if self.cooldown and last is not None and timedelta(0) <= ts - last < self.cooldown:
            return

        now = datetime.now(UTC)
        sent_at = event.get("sent_at")
        delay = (now - datetime.fromisoformat(sent_at)).total_seconds() if sent_at else None
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
            dedup_key=f"protective_stop:{event['event_id']}",
        )
        if send(alert):
            self.alerts_sent += 1
            self.last_alert_event_ts[robot] = ts
            if delay is not None:
                self.max_delay_seconds = max(self.max_delay_seconds, delay)
