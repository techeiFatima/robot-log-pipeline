"""Tests for alerts, monitoring checks and the protective-stop business alert.
SYNTHETIC data only; the webhook is a local HTTP server started by the test."""

import json
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb
import pytest

from robot_pipeline import alerts, monitor
from robot_pipeline.alerts import Alert, read_alerts, send
from robot_pipeline.loader import load_file
from robot_pipeline.stream.events import decode, encode, row_to_event
from robot_pipeline.stream.stop_alerts import StopAlerter
from robot_pipeline.stream.store import StreamStore
from robot_pipeline.synthetic import write_sample


@pytest.fixture(autouse=True)
def alert_log(tmp_path, monkeypatch):
    path = tmp_path / "alerts.log"
    monkeypatch.setenv("ALERT_LOG_PATH", str(path))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    return path


@pytest.fixture
def webhook(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append(json.loads(body))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/hook")
    yield received
    server.shutdown()


# ---------- alerts ----------


def test_alert_is_written_to_the_log_by_default(alert_log):
    assert send(Alert("x", "warning", "something happened"))
    [rec] = read_alerts(alert_log)
    assert rec["name"] == "x" and rec["webhook"] == "not configured"


def test_alert_is_posted_to_the_webhook_when_configured(webhook, alert_log):
    send(Alert("x", "critical", "boom", {"a": 1}))
    assert webhook[0]["name"] == "x" and webhook[0]["text"] == "[CRITICAL] x: boom"
    assert read_alerts(alert_log)[0]["webhook"].startswith("sent")


def test_slack_format_sends_only_text(webhook, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_FORMAT", "slack")
    send(Alert("x", "info", "hello"))
    assert webhook == [{"text": "[INFO] x: hello"}]


def test_a_broken_webhook_never_raises(monkeypatch, alert_log):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/unreachable")
    assert send(Alert("x", "critical", "still logged"))
    assert read_alerts(alert_log)[0]["webhook"].startswith("failed")


def test_cooldown_suppresses_repeats(alert_log):
    assert send(Alert("x", "warning", "1", dedup_key="k"), cooldown_seconds=3600)
    assert not send(Alert("x", "warning", "2", dedup_key="k"), cooldown_seconds=3600)
    assert len(read_alerts(alert_log)) == 1


def test_invalid_severity_is_rejected():
    with pytest.raises(ValueError):
        Alert("x", "urgent", "m")


def test_cli_sends_an_alert(alert_log):
    alerts.main(["--name", "pipeline_task_failed", "--message", "load failed", "--dedup-key", "r1"])
    assert read_alerts(alert_log)[0]["severity"] == "critical"


# ---------- monitoring checks ----------


@pytest.fixture
def loaded_db(tmp_path):
    db = tmp_path / "m.duckdb"
    load_file(write_sample(tmp_path / "SYNTHETIC_a.xlsx", rows=100, seed=1), db)
    return db


def run(db, tmp_path, now, **kw):
    state = tmp_path / "state"
    state.mkdir(exist_ok=True)
    return {r.name: r for r in monitor.run_checks(db, state, tmp_path / "target", now, **kw)}


def test_fresh_data_is_ok_and_old_data_alerts(loaded_db, tmp_path):
    now = datetime.now(UTC)
    assert run(loaded_db, tmp_path, now)["data_stopped_arriving"].ok
    stale = run(loaded_db, tmp_path, now + timedelta(hours=30))["data_stopped_arriving"]
    assert not stale.ok and stale.severity == "critical"


def test_reloading_an_unchanged_file_does_not_hide_stale_data(loaded_db, tmp_path):
    load_file(tmp_path / "SYNTHETIC_a.xlsx", loaded_db)  # daily re-run, same file
    stale = run(loaded_db, tmp_path, datetime.now(UTC) + timedelta(hours=30))
    assert not stale["data_stopped_arriving"].ok


def test_missing_database_alerts(tmp_path):
    res = run(tmp_path / "nope.duckdb", tmp_path, datetime.now(UTC))
    assert not res["data_stopped_arriving"].ok


def test_row_count_low_needs_history_then_alerts(tmp_path):
    db = tmp_path / "v.duckdb"
    for i in range(4):
        load_file(write_sample(tmp_path / f"SYNTHETIC_{i}.xlsx", rows=200, seed=i), db)
    assert run(db, tmp_path, datetime.now(UTC))["row_count_low"].ok
    load_file(write_sample(tmp_path / "SYNTHETIC_small.xlsx", rows=20, seed=9), db)
    low = run(db, tmp_path, datetime.now(UTC))["row_count_low"]
    assert not low.ok and low.details["latest_rows"] == 20


def test_too_little_history_is_not_an_alert(loaded_db, tmp_path):
    assert run(loaded_db, tmp_path, datetime.now(UTC))["row_count_low"].ok


def test_hourly_rows_low(tmp_path):
    db = tmp_path / "h.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE agg_stops_by_hour AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2030-01-01 08:00', 3600), (TIMESTAMP '2030-01-01 09:00', 3500), "
        "(TIMESTAMP '2030-01-01 10:00', 400), (TIMESTAMP '2030-01-01 11:00', 3550)"
        ") t(hour_utc, rows_total)"
    )
    con.close()
    res = monitor.check_hourly_volume(duckdb.connect(str(db), read_only=True), monitor.Settings())
    assert not res.ok and res.details["low_hours"] == [("2030-01-01T10:00:00", 400)]


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def test_failed_success_check_alerts(tmp_path):
    write_json(
        tmp_path / "s" / "checks" / "success_check_latest.json",
        {"passed": False, "failures": ["1 raw stop rows are not stops in clean"]},
    )
    res = monitor.check_failed_checks(tmp_path / "s", tmp_path / "t")
    assert not res.ok and "success test failed" in res.message


def test_failed_dbt_test_alerts(tmp_path):
    write_json(tmp_path / "s" / "checks" / "success_check_latest.json", {"passed": True})
    write_json(
        tmp_path / "t" / "run_results.json",
        {
            "results": [
                {"unique_id": "test.robot_logs.not_null_x", "status": "fail"},
                {"unique_id": "model.robot_logs.y", "status": "success"},
            ]
        },
    )
    res = monitor.check_failed_checks(tmp_path / "s", tmp_path / "t")
    assert not res.ok and res.details["dbt_failures"] == ["test.robot_logs.not_null_x"]


def test_failed_pipeline_run_alerts_even_if_old_report_passed(tmp_path):
    write_json(tmp_path / "s" / "checks" / "success_check_latest.json", {"passed": True})
    write_json(
        tmp_path / "s" / "pipeline_run_latest.json",
        {"status": "failed", "run_id": "r9", "failed_task": "load_raw_logs"},
    )
    res = monitor.check_failed_checks(tmp_path / "s", tmp_path / "t")
    assert not res.ok and "load_raw_logs" in res.message


def test_all_passing_checks_are_ok(tmp_path):
    write_json(tmp_path / "s" / "checks" / "success_check_latest.json", {"passed": True})
    write_json(tmp_path / "s" / "pipeline_run_latest.json", {"status": "success"})
    assert monitor.check_failed_checks(tmp_path / "s", tmp_path / "t").ok


def test_stream_silence(tmp_path):
    store = StreamStore(tmp_path / "st.duckdb")
    store.handle(make_event(1, T0))
    store.close()
    now = datetime.now(UTC)
    s = monitor.Settings()
    assert monitor.check_stream_silence(tmp_path / "st.duckdb", now, s).ok
    assert not monitor.check_stream_silence(
        tmp_path / "st.duckdb", now + timedelta(minutes=10), s
    ).ok


def test_monitor_sends_alerts_once_per_day(loaded_db, tmp_path, alert_log):
    s = monitor.Settings()
    now = datetime.now(UTC) + timedelta(hours=30)
    results = run(loaded_db, tmp_path, now)
    first = monitor.alert_on(list(results.values()), s, now)
    second = monitor.alert_on(list(results.values()), s, now)
    assert first >= 1 and second == 0


# ---------- protective-stop business alert ----------

T0 = datetime(2030, 1, 1, 8, 0, 0)


def make_event(n, ts, stop=0.0):
    row = {
        "Num": n,
        "Timestamp": ts.isoformat() + "Z",
        "Robot_ProtectiveStop": stop,
        "cycle ": 1,
        "grip_lost": False,
    }
    return decode(encode(row_to_event(row, "SYNTHETIC.xlsx")))


def feed(tmp_path, rows, **kw):
    tmp_path.mkdir(parents=True, exist_ok=True)
    store = StreamStore(tmp_path / "s.duckdb")
    alerter = StopAlerter(store, **kw)
    for n, sec, stop in rows:
        ev = make_event(n, T0 + timedelta(seconds=sec), stop)
        alerter(ev, store.handle(ev))
    store.close()
    return alerter


def test_one_alert_per_stop_episode(tmp_path, alert_log):
    a = feed(tmp_path, [(1, 0, 0.0), (2, 1, 1.0), (3, 2, 1.0), (4, 3, 0.0), (5, 30, 1.0)])
    assert a.alerts_sent == 2
    recs = read_alerts(alert_log)
    assert [r["name"] for r in recs] == ["protective_stop", "protective_stop"]
    assert all(r["details"]["within_target"] for r in recs)
    assert all(r["details"]["seconds_from_sent_to_alert"] < 300 for r in recs)


def test_duplicate_stop_does_not_alert_twice(tmp_path):
    a = feed(tmp_path, [(1, 0, 1.0), (1, 0, 1.0)])
    assert a.alerts_sent == 1


def test_no_stop_no_alert_and_unknown_is_not_a_stop(tmp_path):
    assert feed(tmp_path, [(1, 0, 0.0), (2, 1, None)]).alerts_sent == 0


def test_cooldown_limits_bursts(tmp_path):
    rows = [(1, 0, 1.0), (2, 20, 1.0), (3, 40, 1.0), (4, 400, 1.0)]
    assert feed(tmp_path, rows).alerts_sent == 4
    assert feed(tmp_path / "b", rows, cooldown_seconds=300).alerts_sent == 2
