"""Raw files in AWS S3 (NOT RUN against real AWS: needs an account; tested with moto).

Layout in the bucket (``ROBOT_S3_BUCKET``, optional ``ROBOT_S3_PREFIX``):

    <prefix>/raw/robot_logs/ingest_date=YYYY-MM-DD/<file>.xlsx     original file, unchanged
    <prefix>/staged/robot_logs/ingest_date=YYYY-MM-DD/<file>.csv   same rows as CSV, for
                                                                  Snowflake COPY INTO
                                                                  (it cannot read .xlsx)

* A file is validated with the loader's rules before upload, so a broken
  file never lands in the bucket.
* Upload is idempotent: if the object already exists with the same SHA-256
  (stored as object metadata), it is skipped.
* Credentials are never in code: boto3's standard chain is used
  (AWS_PROFILE, environment variables, SSO, or an IAM role).

Usage:
    python -m robot_pipeline.cloud.s3 upload                    # every .xlsx in data/raw/
    python -m robot_pipeline.cloud.s3 download --date 2026-11-26  # back into data/raw/
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from robot_pipeline import config
from robot_pipeline.loader import find_source_files, read_source

log = logging.getLogger(__name__)
DATASET = "robot_logs"


def bucket_name() -> str:
    bucket = os.environ.get("ROBOT_S3_BUCKET")
    if not bucket:
        raise SystemExit("Set ROBOT_S3_BUCKET to the name of your S3 bucket.")
    return bucket


def prefix() -> str:
    return os.environ.get("ROBOT_S3_PREFIX", "").strip("/")


def _key(*parts: str) -> str:
    return "/".join(p for p in (prefix(), *parts) if p)


def raw_key(filename: str, ingest_date: date) -> str:
    return _key("raw", DATASET, f"ingest_date={ingest_date.isoformat()}", filename)


def staged_key(filename: str, ingest_date: date) -> str:
    stem = Path(filename).stem
    return _key("staged", DATASET, f"ingest_date={ingest_date.isoformat()}", f"{stem}.csv")


def to_staged_csv(df: pd.DataFrame, source_file: str) -> bytes:
    """The rows as CSV: same column names and values, plus source_file.

    Missing values become empty fields (Snowflake reads them as NULL). Values
    that contain quotes, like the stray-quoted timestamps, are escaped by the
    CSV writer and come back unchanged.
    """
    out = df.copy()
    out["source_file"] = source_file
    buf = io.StringIO()
    out.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def _client(client=None):
    if client is not None:
        return client
    import boto3

    return boto3.client("s3")


def _existing_sha(client, bucket: str, key: str) -> str | None:
    from botocore.exceptions import ClientError

    try:
        return client.head_object(Bucket=bucket, Key=key)["Metadata"].get("sha256")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


@dataclass
class UploadResult:
    source_file: str
    raw_key: str
    staged_key: str
    uploaded: bool  # False if the same content was already there


def upload_file(path: Path, bucket: str, ingest_date: date | None = None, client=None):
    path = Path(path)
    df = read_source(path)  # validate first: same rules as the loader
    body = path.read_bytes()
    sha = hashlib.sha256(body).hexdigest()
    ingest_date = ingest_date or datetime.now(UTC).date()
    s3 = _client(client)
    rkey, skey = raw_key(path.name, ingest_date), staged_key(path.name, ingest_date)
    if _existing_sha(s3, bucket, rkey) == sha and _existing_sha(s3, bucket, skey) == sha:
        return UploadResult(path.name, rkey, skey, uploaded=False)
    meta = {"sha256": sha, "source_file": path.name}
    s3.put_object(Bucket=bucket, Key=rkey, Body=body, Metadata=meta)
    s3.put_object(
        Bucket=bucket,
        Key=skey,
        Body=to_staged_csv(df, path.name),
        Metadata=meta,
        ContentType="text/csv",
    )
    return UploadResult(path.name, rkey, skey, uploaded=True)


def download_raw(bucket: str, ingest_date: date, dest: Path, client=None) -> list[Path]:
    s3 = _client(client)
    folder = _key("raw", DATASET, f"ingest_date={ingest_date.isoformat()}") + "/"
    dest.mkdir(parents=True, exist_ok=True)
    paths = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
        for obj in page.get("Contents", []):
            target = dest / Path(obj["Key"]).name
            s3.download_file(bucket, obj["Key"], str(target))
            paths.append(target)
    return paths


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Raw robot log files in S3 (needs AWS credentials).")
    sub = p.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upload")
    up.add_argument("--raw-dir", type=Path, default=config.raw_dir())
    up.add_argument("--date", type=date.fromisoformat, help="ingest date (default: today, UTC)")
    down = sub.add_parser("download")
    down.add_argument("--date", type=date.fromisoformat, required=True)
    down.add_argument("--dest", type=Path, default=config.raw_dir())
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    bucket = bucket_name()
    if args.cmd == "upload":
        for path in find_source_files(args.raw_dir):
            r = upload_file(path, bucket, args.date)
            state = "uploaded" if r.uploaded else "unchanged, skipped"
            log.info("%s: %s -> s3://%s/%s", r.source_file, state, bucket, r.raw_key)
    else:
        for path in download_raw(bucket, args.date, args.dest):
            log.info("Downloaded %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
