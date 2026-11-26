"""The event format sent over Kafka: one JSON message per log row."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any

TOPIC = "robot.logs.v1"
ROBOT_ID = "ur3-cobot-01"  # a made-up id for the one robot in the dataset
SCHEMA_VERSION = 1


def parse_event_ts(raw: str) -> datetime:
    """Parse the source timestamp text (stray quotes and 'Z' allowed) as naive UTC."""
    text = str(raw).strip().strip('"').strip()
    if text.endswith("Z"):
        text = text[:-1]
    return datetime.fromisoformat(text)


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):  # numpy scalars
        return value.item()
    return value


def row_to_event(row: dict[str, Any], source_file: str) -> dict[str, Any]:
    """Wrap one raw row (source column names, values unchanged) as an event."""
    payload = {k: _clean(v) for k, v in row.items() if k not in ("source_file", "loaded_at")}
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{source_file}:{int(payload['Num'])}",
        "robot_id": ROBOT_ID,
        "source_file": source_file,
        "row_num": int(payload["Num"]),
        "event_ts": parse_event_ts(payload["Timestamp"]).isoformat(timespec="milliseconds"),
        "payload": payload,
    }


def encode(event: dict[str, Any], sent_at: datetime | None = None) -> bytes:
    body = dict(event, sent_at=(sent_at or datetime.now(UTC)).isoformat())
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


REQUIRED_FIELDS = ("event_id", "robot_id", "source_file", "row_num", "event_ts", "payload")


class BadEvent(ValueError):
    """A message that is not a valid event (it goes to stream_rejects)."""


def decode(raw: bytes) -> dict[str, Any]:
    try:
        event = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BadEvent(f"not valid JSON: {exc}") from exc
    if not isinstance(event, dict):
        raise BadEvent("event is not a JSON object")
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        raise BadEvent(f"missing fields: {missing}")
    try:
        event["event_ts_parsed"] = datetime.fromisoformat(event["event_ts"])
    except (TypeError, ValueError) as exc:
        raise BadEvent(f"bad event_ts: {event.get('event_ts')!r}") from exc
    return event
