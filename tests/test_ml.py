"""Tests for the stop-prediction model: the split, the metrics, and no look-ahead.
SYNTHETIC data only."""

from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from robot_pipeline.ml import train  # noqa: E402

T0 = datetime(2030, 1, 1, 8, 0, 0)


def frame(n=100, label_every=10):
    return pd.DataFrame(
        {
            "event_ts": [T0 + timedelta(seconds=i) for i in range(n)],
            "row_num": range(1, n + 1),
            train.LABEL: [1 if i % label_every in (7, 8, 9) else 0 for i in range(n)],
            "next_stop_event_id": [f"stop-{i // label_every}" for i in range(n)],
        }
    )


def test_time_split_keeps_order_and_purges_the_horizon():
    df = frame(200)
    tr, te, cut = train.time_split(df, test_fraction=0.3)
    assert tr["event_ts"].max() < cut - timedelta(seconds=train.HORIZON_S)
    assert te["event_ts"].min() >= cut
    assert len(te) == 60
    assert set(tr.index).isdisjoint(te.index)


def test_episode_recall_counts_episodes_not_rows():
    df = frame(30)  # positives at rows 8-10, 18-20, 28-30: three episodes
    flagged = np.zeros(30, dtype=int)
    flagged[8] = 1  # one warning, before the second episode
    assert train.episode_recall(df, flagged) == (1, 3)


def test_a_missing_row_does_not_split_an_episode():
    df = frame(30).drop(index=8).reset_index(drop=True)  # e.g. an unknown-status row
    assert train.episode_recall(df, np.zeros(len(df), dtype=int)) == (0, 3)


def test_a_missing_row_does_not_split_an_alarm():
    df = frame(30)
    flagged = np.zeros(30, dtype=bool)
    flagged[4:9] = True  # one warning run that reaches the positives at 7-8
    df, flagged = df.drop(index=5).reset_index(drop=True), np.delete(flagged, 5)
    assert train.false_alarm_runs(df, flagged) == 0


def test_false_alarm_runs_count_runs_not_rows():
    df = frame(30)
    flagged = np.zeros(30, dtype=bool)
    flagged[0:4] = True  # one run, no stop ahead -> 1 false alarm
    flagged[7:9] = True  # one run with positives -> not a false alarm
    assert train.false_alarm_runs(df, flagged) == 1


def test_recorded_hours_skips_gaps():
    df = frame(3601)
    df.loc[1800:, "event_ts"] += timedelta(hours=5)  # one 5-hour gap
    assert abs(train.recorded_hours(df) - 3599 / 3600) < 1e-9


@pytest.mark.integration
def test_features_never_look_ahead(tmp_path):
    """Delete the later rows from the source and rebuild: every feature of the
    remaining early rows must be unchanged. Only the label may look ahead."""
    from robot_pipeline.dbt_runner import run_dbt
    from robot_pipeline.loader import load_file
    from robot_pipeline.synthetic import make_frame

    feature_cols = ", ".join(train.FEATURES)

    def build(df, name):
        db = tmp_path / f"{name}.duckdb"
        path = tmp_path / name / "SYNTHETIC.xlsx"
        path.parent.mkdir()
        df.to_excel(path, index=False)
        load_file(path, db)
        args = ["build", "--target-path", str(tmp_path / name)]
        assert run_dbt(args, db_path=db) == 0
        con = duckdb.connect(str(db), read_only=True)
        out = con.execute(
            f"SELECT row_num, {feature_cols} FROM ml_stop_features WHERE row_num <= 250 "
            "ORDER BY row_num"
        ).df()
        con.close()
        return out

    full = make_frame(rows=400, seed=11, gap_at=1000)
    a = build(full, "full")
    # Cut the source right after the compared rows (<= 250): any feature that
    # peeked even one row ahead would now see different data. (Checked: a
    # feature changed to lead(..., 10) makes this test fail.)
    b = build(full.iloc[:251], "cut")
    pd.testing.assert_frame_equal(a, b)


@pytest.mark.integration
def test_training_runs_end_to_end_on_synthetic_data(tmp_path):
    from robot_pipeline.dbt_runner import run_dbt
    from robot_pipeline.loader import load_file
    from robot_pipeline.synthetic import write_sample

    db = tmp_path / "ml.duckdb"
    load_file(write_sample(tmp_path / "SYNTHETIC.xlsx", rows=1200, seed=5, gap_at=5000), db)
    assert run_dbt(["build", "--target-path", str(tmp_path / "t")], db_path=db) == 0
    report = train.run(db)
    names = [r["name"] for r in report["results"]]
    assert names == ["base_rate", "rule_recent_stop", "logistic_regression", "gradient_boosting"]
    assert report["train"]["to"] < report["test"]["from"]
    assert "Gradient-boosted trees" in train.to_markdown(report)
