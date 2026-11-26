"""Tests for the simulated stream. Store and disorder logic need no broker;
the last test needs a running Kafka broker and is skipped without one."""

import os
import socket
import uuid
from datetime import datetime, timedelta

import duckdb
import pytest

from robot_pipeline.stream.events import BadEvent, decode, encode, parse_event_ts, row_to_event
from robot_pipeline.stream.producer import load_events, simulate_disorder
from robot_pipeline.stream.store import Outcome, StreamStore


def make_event(n: int, ts: datetime, stop: float | None = 0.0) -> dict:
    row = {"Num": n, "Timestamp": ts.isoformat() + "Z", "Robot_ProtectiveStop": stop}
    return decode(encode(row_to_event(row, "SYNTHETIC.xlsx")))


T0 = datetime(2030, 1, 1, 8, 0, 0)


@pytest.fixture
def store(tmp_path):
    s = StreamStore(tmp_path / "stream.duckdb", allowed_lateness_seconds=10)
    yield s
    s.close()


def test_timestamp_parsing_handles_stray_quotes():
    assert parse_event_ts('"2022-10-26T08:20:35.838Z"') == datetime(2022, 10, 26, 8, 20, 35, 838000)
    assert parse_event_ts("2022-10-26T08:17:21.847Z") == datetime(2022, 10, 26, 8, 17, 21, 847000)


def test_event_keeps_raw_values_and_nan_becomes_null():
    ev = row_to_event(
        {"Num": 3, "Timestamp": '"2030-01-01T08:00:00.000Z"', "Robot_ProtectiveStop": float("nan")},
        "f.xlsx",
    )
    assert ev["event_id"] == "f.xlsx:3"
    assert ev["payload"]["Timestamp"] == '"2030-01-01T08:00:00.000Z"'
    assert ev["payload"]["Robot_ProtectiveStop"] is None


def test_in_order_events_are_stored(store):
    for i in range(3):
        assert store.handle(make_event(i + 1, T0 + timedelta(seconds=i))) == Outcome.STORED
    assert store.summary()["events_stored"] == 3


def test_event_arriving_twice_is_stored_once_and_counted(store):
    ev = make_event(1, T0)
    assert store.handle(ev) == Outcome.STORED
    assert store.handle(ev) == Outcome.DUPLICATE
    s = store.summary()
    assert s["events_stored"] == 1
    assert s["duplicates_ignored"] == 1


def test_out_of_order_event_is_stored_and_flagged(store):
    store.handle(make_event(2, T0 + timedelta(seconds=2)))
    assert store.handle(make_event(1, T0 + timedelta(seconds=1))) == Outcome.OUT_OF_ORDER
    s = store.summary()
    assert s["events_stored"] == 2 and s["out_of_order"] == 1 and s["late"] == 0


def test_late_event_is_kept_not_dropped(store):
    store.handle(make_event(100, T0 + timedelta(seconds=100)))
    assert store.handle(make_event(1, T0)) == Outcome.LATE
    s = store.summary()
    assert s["events_stored"] == 2 and s["late"] == 1
    (lateness,) = store.con.execute(
        "SELECT lateness_seconds FROM stream_events WHERE row_num = 1"
    ).fetchone()
    assert lateness == 100


def test_watermark_survives_restart(tmp_path):
    path = tmp_path / "s.duckdb"
    s1 = StreamStore(path)
    s1.handle(make_event(50, T0 + timedelta(seconds=50)))
    s1.close()
    s2 = StreamStore(path)
    assert s2.handle(make_event(1, T0)) == Outcome.LATE
    assert s2.handle(make_event(50, T0 + timedelta(seconds=50))) == Outcome.DUPLICATE
    s2.close()


def test_bad_messages_are_rejected_not_crashing(store):
    not_object_payload = (
        b'{"event_id":"a","robot_id":"r","source_file":"f","row_num":1,'
        b'"event_ts":"2030-01-01T00:00:00","payload":[1]}'
    )
    for raw in [b"not json", b"[1,2]", b'{"event_id": "x"}']:
        with pytest.raises(BadEvent):
            decode(raw)
        store.reject("bad", raw)
    with pytest.raises(BadEvent, match="payload"):
        decode(not_object_payload)
    assert store.summary()["rejected"] == 3


def test_aware_and_naive_timestamps_are_compared_safely(store):
    store.handle(make_event(1, T0))  # naive UTC 08:00:00
    raw = row_to_event({"Num": 2, "Timestamp": "2030-01-01T08:00:05Z"}, "SYNTHETIC.xlsx")
    raw["event_ts"] = "2030-01-01T08:00:05+02:00"  # = 06:00:05 UTC, an aware timestamp
    # Two hours older than the watermark: late, and no naive/aware TypeError.
    assert store.handle(decode(encode(raw))) == Outcome.LATE
    assert parse_event_ts("2030-01-01T10:00:00+02:00") == datetime(2030, 1, 1, 8, 0, 0)


def test_redelivered_duplicate_is_counted_once(store):
    ev = make_event(1, T0)
    store.handle(ev, "t", 0, 0)
    store.handle(ev, "t", 0, 5)  # a real duplicate message at offset 5
    store.handle(ev, "t", 0, 5)  # the same Kafka record delivered again after a crash
    assert store.summary()["duplicates_ignored"] == 1


def test_stop_and_unknown_status_are_counted(store):
    store.handle(make_event(1, T0, stop=1.0))
    store.handle(make_event(2, T0 + timedelta(seconds=1), stop=None))
    s = store.summary()
    assert s["stop_events"] == 1 and s["unknown_status"] == 1


def test_disorder_injects_problems_but_loses_nothing():
    events = [
        {"event_id": str(i), "event_ts": (T0 + timedelta(seconds=i)).isoformat()}
        for i in range(500)
    ]
    sent, injected = simulate_disorder(events, 0.05, 0.05, 0.02, late_by=30, seed=1)
    assert injected.duplicates > 0 and injected.reordered > 0 and injected.delayed > 0
    assert len(sent) == len(events) + injected.duplicates
    assert {e["event_id"] for e in sent} == {e["event_id"] for e in events}
    assert sent != events


def test_disorder_is_deterministic():
    events = [{"event_id": str(i)} for i in range(100)]
    a, _ = simulate_disorder(events, 0.1, 0.1, 0.1, seed=3)
    b, _ = simulate_disorder(events, 0.1, 0.1, 0.1, seed=3)
    assert a == b


def test_disordered_replay_stores_each_row_once(store, sample_file):
    """Full replay logic without a broker: what the consumer would store."""
    events = load_events(sample_file)
    sent, injected = simulate_disorder(events, 0.05, 0.05, 0.03, late_by=30, seed=2)
    for ev in sent:
        store.handle(decode(encode(ev)))
    s = store.summary()
    assert s["events_stored"] == len(events)
    assert s["duplicates_ignored"] == injected.duplicates
    assert s["late"] > 0 and s["out_of_order"] >= s["late"]


def kafka_available(servers: str) -> bool:
    host, _, port = servers.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.kafka
def test_end_to_end_through_a_real_broker(tmp_path, sample_file):
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    if os.environ.get("REQUIRE_KAFKA"):
        import confluent_kafka  # noqa: F401  (an import error must fail, not skip)
    else:
        pytest.importorskip("confluent_kafka")
    if not kafka_available(servers):
        if os.environ.get("REQUIRE_KAFKA"):
            pytest.fail(f"REQUIRE_KAFKA is set but no Kafka broker at {servers}")
        pytest.skip(f"no Kafka broker at {servers}")
    from robot_pipeline.stream.consumer import consume
    from robot_pipeline.stream.producer import ensure_topic, produce

    topic = f"test.robot.logs.{uuid.uuid4().hex[:8]}"
    ensure_topic(servers, topic)
    events = load_events(sample_file)
    sent, injected = simulate_disorder(events, 0.05, 0.05, 0.03, late_by=30, seed=4)
    assert produce(sent, servers, topic) == len(sent)

    store = StreamStore(tmp_path / "stream.duckdb")
    seen = []
    handled = consume(
        store,
        servers,
        topic,
        group_id=f"g-{uuid.uuid4().hex[:6]}",
        idle_timeout=5,
        hooks=[lambda e, o: seen.append(o)],
    )
    s = store.summary()
    store.close()
    assert handled == len(sent)
    assert s["events_stored"] == len(events)
    assert s["duplicates_ignored"] == injected.duplicates

    raw = duckdb.connect()
    raw_stops = sum(1 for e in events if e["payload"]["Robot_ProtectiveStop"] == 1)
    raw.close()
    assert s["stop_events"] == raw_stops


@pytest.mark.kafka
def test_a_message_that_fails_is_not_skipped_after_restart(tmp_path, sample_file):
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    if os.environ.get("REQUIRE_KAFKA"):
        import confluent_kafka  # noqa: F401
    else:
        pytest.importorskip("confluent_kafka")
    if not kafka_available(servers):
        if os.environ.get("REQUIRE_KAFKA"):
            pytest.fail(f"REQUIRE_KAFKA is set but no Kafka broker at {servers}")
        pytest.skip(f"no Kafka broker at {servers}")
    from robot_pipeline.stream.consumer import consume
    from robot_pipeline.stream.producer import ensure_topic, produce

    topic = f"test.robot.fail.{uuid.uuid4().hex[:8]}"
    group = f"g-{uuid.uuid4().hex[:6]}"
    ensure_topic(servers, topic)
    events = load_events(sample_file)[:60]
    produce(events, servers, topic)
    failing_id = events[49]["event_id"]

    def crash_on_50th(event, outcome):
        if event["event_id"] == failing_id:
            raise RuntimeError("simulated crash while handling")

    store = StreamStore(tmp_path / "s.duckdb")
    with pytest.raises(RuntimeError):
        consume(
            store, servers, topic, group, idle_timeout=5, commit_every=10, hooks=[crash_on_50th]
        )
    seen = []
    consume(
        store,
        servers,
        topic,
        group,
        idle_timeout=5,
        hooks=[lambda e, o: seen.append(e["event_id"])],
    )
    s = store.summary()
    store.close()
    assert failing_id in seen  # read again, not skipped
    assert s["events_stored"] == 60
