"""Generate a small SYNTHETIC log file with the same shape as the UCI dataset.

THE VALUES ARE RANDOM AND NOT REAL ROBOT DATA. The file exists so tests and CI
can run the whole pipeline without downloading or committing the real dataset.

It reproduces the source's quirks on purpose: text timestamps ending in "Z",
some wrapped in stray double quotes, ~1.005 s spacing with one recording gap,
the "cycle " column with its trailing space, Temperature_T0, a 0/1 DOUBLE stop
flag, and a few rows with no stop value and no sensor data.

Usage:
    python -m robot_pipeline.synthetic data/sample/SYNTHETIC_cobot_logs.xlsx --rows 300
"""

from __future__ import annotations

import argparse
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from robot_pipeline.loader import EXPECTED_COLUMNS

SYNTHETIC_LABEL = "SYNTHETIC SAMPLE - random values, NOT real robot data"
SENSOR_COLUMNS = [
    c
    for c in EXPECTED_COLUMNS
    if c.startswith(("Current_", "Temperature_", "Speed_")) or c == "Tool_current"
]


def make_frame(
    rows: int = 300,
    seed: int = 42,
    start: datetime = datetime(2030, 1, 1, 8, 0, 0, tzinfo=UTC),
    stop_every: int = 25,
    unknown_every: int = 60,
    quote_every: int = 9,
    gap_at: int | None = None,
) -> pd.DataFrame:
    """Build the synthetic rows as a DataFrame with the exact source columns."""
    rng = random.Random(seed)
    gap_at = rows // 2 if gap_at is None else gap_at
    ts = start
    records = []
    for i in range(1, rows + 1):
        if i > 1:
            ts += timedelta(milliseconds=1005 + rng.randint(-20, 20))
        if i == gap_at:
            ts += timedelta(minutes=30)  # one recording gap
        text = ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
        if i % quote_every == 0:
            text = f'"{text}"'
        unknown = i % unknown_every == 0
        is_stop = (i % stop_every) in (0, 1) and not unknown
        rec: dict[str, object] = {"Num": i, "Timestamp": text}
        for col in SENSOR_COLUMNS:
            if unknown:
                rec[col] = None
            elif col.startswith("Temperature"):
                rec[col] = round(30 + rng.random() * 10, 4)
            elif col.startswith("Speed"):
                rec[col] = round(rng.uniform(-0.6, 0.6), 6)
            elif col == "Tool_current":
                rec[col] = round(rng.uniform(0.02, 0.6), 6)
            else:
                rec[col] = round(rng.uniform(-6, 6), 6)
        rec["cycle "] = 1 + (i - 1) // 20
        rec["Robot_ProtectiveStop"] = None if unknown else (1.0 if is_stop else 0.0)
        rec["grip_lost"] = False if unknown else rng.random() < 0.03
        records.append(rec)
    df = pd.DataFrame.from_records(records, columns=list(EXPECTED_COLUMNS))
    for col in [*SENSOR_COLUMNS, "Robot_ProtectiveStop"]:
        df[col] = df[col].astype("float64")
    return df


def write_sample(path: Path, **kwargs) -> Path:
    """Write the synthetic frame to an Excel file labelled as synthetic."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    make_frame(**kwargs).to_excel(path, index=False, sheet_name="SYNTHETIC")
    wb = load_workbook(path)
    wb.properties.title = SYNTHETIC_LABEL
    wb.properties.description = SYNTHETIC_LABEL
    wb.save(path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Write a SYNTHETIC sample log file.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    out = write_sample(args.path, rows=args.rows, seed=args.seed)
    print(f"Wrote {out} ({args.rows} rows). {SYNTHETIC_LABEL}.")


if __name__ == "__main__":
    main()
