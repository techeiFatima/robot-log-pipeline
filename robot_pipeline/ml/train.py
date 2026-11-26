"""Train and honestly evaluate a model that predicts protective stops.

Question: given what the robot logged up to now, will a NEW protective-stop
episode start within the next 10 seconds?

* Data: dbt model ``ml_stop_features`` (rows not already stopped, known status,
  complete 10-second label window). Features look only backwards.
* Split by TIME, never at random: the earliest 70 % of rows train, the latest
  30 % test. Training rows whose 10-second label window reaches into the test
  period are dropped (a "purge"), so no test information leaks into training.
* Models are fit on the first 80 % of the training period; the decision
  threshold and the "best model" are chosen on the last 20 % (validation).
  The SAME fitted model is then scored on test, so the threshold belongs to
  the model it is applied to. Nothing is chosen on the test set.
* Compared against two baselines: the base rate, and a simple rule
  ("a stop happened recently, so another is likely").
* Uncertainty: a block bootstrap (1-minute blocks) over the test period, since
  neighbouring rows are not independent.

Usage:
    python -m robot_pipeline.ml.train               # writes state/ml/model_report.json
    python -m robot_pipeline.ml.train --write-docs  # also docs/model_report.md
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from robot_pipeline import config

LABEL = "stop_within_horizon"
FEATURES = [
    *[f"current_j{i}" for i in range(6)],
    *[f"temperature_j{i}" for i in range(6)],
    *[f"speed_j{i}" for i in range(6)],
    "tool_current",
    "avg_joint_temp",
    "total_abs_current",
    "total_abs_speed",
    "grip_lost_now",
    "total_abs_current_avg10",
    "total_abs_current_std10",
    "total_abs_speed_avg10",
    "tool_current_avg10",
    "grip_lost_last10",
    "avg_joint_temp_change10",
    "seconds_since_last_stop",
    "stop_episodes_last300",
    "rows_into_cycle",
]
SEED = 0
HORIZON_S = 10
RUN_GAP_S = 2.5


def load(db_path: Path) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            "SELECT * FROM ml_stop_features WHERE label_window_complete ORDER BY event_ts"
        ).df()
    finally:
        con.close()
    df[LABEL] = df[LABEL].astype(int)
    return df


def time_split(df: pd.DataFrame, test_fraction: float = 0.3):
    """Earliest rows -> train, latest -> test; purge train rows whose label
    window overlaps the test period."""
    cut = df["event_ts"].iloc[int(len(df) * (1 - test_fraction))]
    test = df[df["event_ts"] >= cut]
    train = df[df["event_ts"] < cut - pd.Timedelta(seconds=HORIZON_S)]
    return train, test, cut


def models() -> dict:
    return {
        "logistic_regression": make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            LogisticRegression(class_weight="balanced", max_iter=5000, random_state=SEED),
        ),
        "gradient_boosting": HistGradientBoostingClassifier(
            max_depth=3, learning_rate=0.05, max_iter=300, random_state=SEED
        ),
    }


def rule_score(df: pd.DataFrame) -> np.ndarray:
    """Baseline rule: the more recent the last stop, the higher the risk."""
    s = df["seconds_since_last_stop"].fillna(1e9).to_numpy()
    return -s


def best_f1_threshold(y: np.ndarray, score: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(y, score)
    f1 = 2 * precision * recall / np.clip(precision + recall, 1e-12, None)
    return float(thresholds[int(np.argmax(f1[:-1]))])


def episode_recall(test: pd.DataFrame, flagged: np.ndarray) -> tuple[int, int]:
    """Of the stop episodes preceded by at least one test row in the 10 s before
    them, how many got a warning in time? Grouped by the episode each positive
    row points to (next_stop_event_id), so a missing row cannot split an episode."""
    t = test.assign(flagged=flagged)
    warned = t[t[LABEL] == 1].groupby("next_stop_event_id")["flagged"].max()
    return int(warned.sum()), int(len(warned))


def block_bootstrap_ap(y, score, block=60, n=500, seed=SEED) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    blocks = [np.arange(i, min(i + block, len(y))) for i in range(0, len(y), block)]
    stats = []
    for _ in range(n):
        idx = np.concatenate([blocks[j] for j in rng.integers(0, len(blocks), len(blocks))])
        if y[idx].sum() == 0:
            continue
        stats.append(average_precision_score(y[idx], score[idx]))
    lo, hi = np.percentile(stats, [2.5, 97.5])
    return float(lo), float(hi)


@dataclass
class Result:
    name: str
    pr_auc: float
    pr_auc_ci: tuple[float, float]
    roc_auc: float
    threshold: float | None
    precision: float
    recall: float
    episodes_warned: int
    episodes_total: int
    false_alarms_per_recorded_hour: float  # runs of flagged rows with no stop ahead


def false_alarm_runs(test: pd.DataFrame, flagged: np.ndarray) -> int:
    """A person sees a run of consecutive flagged rows as ONE alarm. Count the
    runs that contain no row with a stop coming within the horizon."""
    t = test.assign(flagged=np.asarray(flagged, dtype=bool))
    # A run breaks when a row is not flagged or when more than RUN_GAP_S passes
    # between rows (a single missing row, ~2 s, does not break a run).
    gap = t["event_ts"].diff().dt.total_seconds().fillna(0) > RUN_GAP_S
    new_run = (t["flagged"] & ~t["flagged"].shift(fill_value=False)) | gap
    runs = t[t["flagged"]].groupby(new_run.cumsum()[t["flagged"]])[LABEL].max()
    return int((runs == 0).sum())


def recorded_hours(df: pd.DataFrame) -> float:
    """Hours actually covered by rows (steps longer than 5 s are gaps)."""
    step = df["event_ts"].diff().dt.total_seconds()
    return float(step[step <= 5].sum() / 3600)


def evaluate(name, y, score, threshold, test, hours) -> Result:
    flagged = score >= threshold
    tp = int((flagged & (y == 1)).sum())
    fp = int((flagged & (y == 0)).sum())
    fn = int((~flagged & (y == 1)).sum())
    warned, total = episode_recall(test, flagged.astype(int))
    return Result(
        name=name,
        pr_auc=float(average_precision_score(y, score)),
        pr_auc_ci=block_bootstrap_ap(y, score),
        roc_auc=float(roc_auc_score(y, score)),
        threshold=float(threshold),
        precision=tp / (tp + fp) if tp + fp else 0.0,
        recall=tp / (tp + fn) if tp + fn else 0.0,
        episodes_warned=warned,
        episodes_total=total,
        false_alarms_per_recorded_hour=false_alarm_runs(test, flagged) / hours,
    )


def run(db_path: Path) -> dict:
    df = load(db_path)
    train, test, cut = time_split(df)
    fit, val, _ = time_split(train, test_fraction=0.2)
    y_tr, y_te = train[LABEL].to_numpy(), test[LABEL].to_numpy()
    hours = recorded_hours(test)

    results, fitted = [], {}
    # Base rate: a score that carries no information. Its PR-AUC equals the
    # share of positive rows in the test period; it never warns.
    results.append(
        Result(
            "base_rate",
            float(y_te.mean()),
            (float(y_te.mean()),) * 2,
            0.5,
            None,
            0.0,
            0.0,
            0,
            episode_recall(test, np.zeros(len(test)))[1],
            0.0,
        )
    )
    thr = best_f1_threshold(val[LABEL].to_numpy(), rule_score(val))
    results.append(evaluate("rule_recent_stop", y_te, rule_score(test), thr, test, hours))
    val_pr_auc = {}
    for name, model in models().items():
        model.fit(fit[FEATURES], fit[LABEL])
        val_score = model.predict_proba(val[FEATURES])[:, 1]
        val_pr_auc[name] = float(average_precision_score(val[LABEL], val_score))
        thr = best_f1_threshold(val[LABEL].to_numpy(), val_score)
        score = model.predict_proba(test[FEATURES])[:, 1]  # same model, same threshold
        results.append(evaluate(name, y_te, score, thr, test, hours))
        fitted[name] = model

    best_name = max(val_pr_auc, key=val_pr_auc.get)  # chosen on validation, not test
    imp = permutation_importance(
        fitted[best_name],
        val[FEATURES],
        val[LABEL],
        scoring="average_precision",
        n_repeats=10,
        random_state=SEED,
    )
    importance = sorted(
        zip(FEATURES, imp.importances_mean, imp.importances_std, strict=True),
        key=lambda t: -t[1],
    )[:8]
    return {
        "question": f"Will a new protective-stop episode start within {HORIZON_S} s?",
        "rows_used": len(df),
        "train": {
            "rows": len(train),
            "positives": int(y_tr.sum()),
            "positive_share": round(float(y_tr.mean()), 4),
            "from": str(train["event_ts"].min()),
            "to": str(train["event_ts"].max()),
        },
        "test": {
            "rows": len(test),
            "positives": int(y_te.sum()),
            "positive_share": round(float(y_te.mean()), 4),
            "recorded_hours": round(hours, 3),
            "from": str(test["event_ts"].min()),
            "to": str(test["event_ts"].max()),
        },
        "split_at": str(cut),
        "results": [r.__dict__ for r in results],
        "best_model": best_name,
        "validation_pr_auc": {k: round(v, 4) for k, v in val_pr_auc.items()},
        "fit": {
            "rows": len(fit),
            "from": str(fit["event_ts"].min()),
            "to": str(fit["event_ts"].max()),
        },
        "validation": {
            "rows": len(val),
            "from": str(val["event_ts"].min()),
            "to": str(val["event_ts"].max()),
        },
        "top_features_by_permutation_importance": [
            {"feature": f, "pr_auc_drop": round(float(m), 4), "std": round(float(s), 4)}
            for f, m, s in importance
        ],
    }


def to_markdown(r: dict) -> str:
    """The model report as markdown (written to docs/model_report.md)."""
    names = {
        "base_rate": "Base rate (no model)",
        "rule_recent_stop": "Rule: a stop happened recently",
        "logistic_regression": "Logistic regression",
        "gradient_boosting": "Gradient-boosted trees",
    }
    lines = [
        "# Stop-prediction model: evaluation report",
        "",
        "Generated by `python -m robot_pipeline.ml.train` from the real UCI file; "
        "do not edit by hand.",
        "",
        f"**Question:** {r['question']}",
        "",
        f"- Rows used: {r['rows_used']:,} (rows not already stopped, known status, "
        "complete label window).",
        f"- **Time-based split** at {r['split_at']} UTC. Train: {r['train']['rows']:,} rows "
        f"({r['train']['from']} to {r['train']['to']}), {r['train']['positives']} positive "
        f"({100 * r['train']['positive_share']:.1f} %). Test: {r['test']['rows']:,} rows "
        f"({r['test']['from']} to {r['test']['to']}, {r['test']['recorded_hours']:.2f} h "
        f"recorded), {r['test']['positives']} positive "
        f"({100 * r['test']['positive_share']:.1f} %).",
        "- Training rows whose 10-second label window reaches into the test period were "
        "dropped. The decision threshold (best F1) and the best model were chosen on the "
        "last 20 % of the training period, never on the test set.",
        "",
        "| Approach | PR-AUC (95 % CI) | ROC-AUC | Precision | Recall | Stop episodes "
        "warned in time | False alarms per recorded hour |",
        "|---|---|---|---|---|---|---|",
    ]
    for x in r["results"]:
        lo, hi = x["pr_auc_ci"]
        ci = f"{x['pr_auc']:.3f}" + ("" if x["name"] == "base_rate" else f" ({lo:.3f}–{hi:.3f})")
        lines.append(
            f"| {names.get(x['name'], x['name'])} | {ci} | {x['roc_auc']:.3f} | "
            f"{x['precision']:.3f} | {x['recall']:.3f} | "
            f"{x['episodes_warned']} / {x['episodes_total']} | "
            f"{x['false_alarms_per_recorded_hour']:.1f} |"
        )
    lines += [
        "",
        "PR-AUC (average precision) is the main metric because positives are rare; a model "
        "with no information scores the positive share of the test set. The 95 % interval "
        "is a block bootstrap over 1-minute blocks of the test period. Stop episodes are "
        "counted by the episode each warned row points to. A false alarm is a run of "
        "consecutive warned rows (a run breaks after 2.5 s without a row) with no stop "
        "coming within 10 s.",
        "",
        f"Models were fit on {r['fit']['rows']:,} rows ({r['fit']['from']} to "
        f"{r['fit']['to']}); the threshold and the best model were chosen on "
        f"{r['validation']['rows']:,} validation rows ({r['validation']['from']} to "
        f"{r['validation']['to']}; validation PR-AUC: "
        + ", ".join(f"{names[k]} {v:.3f}" for k, v in r["validation_pr_auc"].items())
        + "). The same fitted model was then scored once on the test period.",
        "",
        f"**Most useful inputs for the best model ({names[r['best_model']]})**, by how much "
        "validation PR-AUC drops when the input is shuffled (permutation importance):",
        "",
        "| Input | PR-AUC drop | ± std |",
        "|---|---|---|",
    ]
    lines += [
        f"| `{f['feature']}` | {f['pr_auc_drop']:.4f} | {f['std']:.4f} |"
        for f in r["top_features_by_permutation_importance"]
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", type=Path, default=config.db_path())
    p.add_argument("--write-docs", action="store_true", help="also write docs/model_report.md")
    args = p.parse_args(argv)
    report = run(args.db)
    out = config.state_dir() / "ml" / "model_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str))
    if args.write_docs:
        md = config.PROJECT_ROOT / "docs" / "model_report.md"
        md.write_text(to_markdown(report))
        print(f"Wrote {md}")
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
