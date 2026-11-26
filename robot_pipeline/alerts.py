"""Send alerts: always to a log file, optionally to a webhook.

* Every alert is appended as one JSON line to ``state/alerts.log``
  (override with ALERT_LOG_PATH). This is the default and needs no setup.
* If ALERT_WEBHOOK_URL is set, the alert is also POSTed there as JSON.
  ALERT_WEBHOOK_FORMAT=slack sends Slack's {"text": ...} shape instead of the
  full alert. A webhook failure is logged, never raised: alerting must not
  crash the pipeline it watches.
* Alerts with the same dedup_key are not repeated within ``cooldown_seconds``.

Standard library only, so it can run anywhere (including from Airflow callbacks).

Command line (used by the Airflow failure callback):
    python -m robot_pipeline.alerts --name pipeline_task_failed --severity critical \\
        --message "load_raw_logs failed" --dedup-key run-123:load_raw_logs
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from robot_pipeline import config

log = logging.getLogger(__name__)

SEVERITIES = ("info", "warning", "critical")


@dataclass
class Alert:
    name: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    dedup_key: str | None = None
    created_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat(timespec="milliseconds")
    )

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}, got {self.severity!r}")

    def text(self) -> str:
        return f"[{self.severity.upper()}] {self.name}: {self.message}"


def alert_log_path() -> Path:
    value = os.environ.get("ALERT_LOG_PATH")
    return Path(value) if value else config.state_dir() / "alerts.log"


def _state_path() -> Path:
    return alert_log_path().with_name("alert_state.json")


def _recently_sent(key: str, cooldown_seconds: float, now: datetime) -> bool:
    try:
        state = json.loads(_state_path().read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    sent = state.get(key)
    return bool(sent) and (now - datetime.fromisoformat(sent)).total_seconds() < cooldown_seconds


def _remember(key: str, now: datetime) -> None:
    path = _state_path()
    try:
        state = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}
    state[key] = now.isoformat()
    path.write_text(json.dumps(state, indent=2))


def _post(url: str, alert: Alert, fmt: str, timeout: float) -> str:
    body = {"text": alert.text()} if fmt == "slack" else {"text": alert.text(), **asdict(alert)}
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return f"sent ({resp.status})"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        log.error("Alert webhook failed: %s", exc)
        return f"failed: {exc}"


def send(alert: Alert, cooldown_seconds: float = 0.0) -> bool:
    """Record (and maybe POST) an alert. Returns False if suppressed by cooldown."""
    now = datetime.now(UTC)
    if (
        alert.dedup_key
        and cooldown_seconds
        and _recently_sent(alert.dedup_key, cooldown_seconds, now)
    ):
        log.info("Suppressed repeat alert %s (cooldown)", alert.dedup_key)
        return False

    path = alert_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = asdict(alert)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        fmt = os.environ.get("ALERT_WEBHOOK_FORMAT", "json").lower()
        record["webhook"] = _post(url, alert, fmt, timeout=5.0)
    else:
        record["webhook"] = "not configured"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
    if alert.dedup_key:
        _remember(alert.dedup_key, now)
    log.warning("ALERT %s", alert.text())
    return True


def read_alerts(path: Path | None = None) -> list[dict[str, Any]]:
    path = path or alert_log_path()
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Send one alert (log file + optional webhook).")
    p.add_argument("--name", required=True)
    p.add_argument("--severity", default="critical", choices=SEVERITIES)
    p.add_argument("--message", required=True)
    p.add_argument("--dedup-key")
    p.add_argument("--details", default="{}", help="JSON object")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    send(Alert(args.name, args.severity, args.message, json.loads(args.details), args.dedup_key))
    return 0


if __name__ == "__main__":
    sys.exit(main())
