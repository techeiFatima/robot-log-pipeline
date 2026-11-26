"""S3 upload/download, tested against moto's in-memory fake of S3.
No AWS account, no network, no real credentials (fake ones are set below)."""

import io
from datetime import date

import pandas as pd
import pytest

pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")
from moto import mock_aws  # noqa: E402

from robot_pipeline.cloud import s3 as cloud_s3  # noqa: E402
from robot_pipeline.loader import LoaderError  # noqa: E402
from robot_pipeline.synthetic import make_frame, write_sample  # noqa: E402

BUCKET = "test-robot-logs"
DAY = date(2030, 1, 2)


@pytest.fixture
def s3(monkeypatch):
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("ROBOT_S3_PREFIX", "dev")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_upload_writes_original_and_staged_csv(s3, sample_file):
    r = cloud_s3.upload_file(sample_file, BUCKET, DAY, client=s3)
    assert r.uploaded
    assert r.raw_key == "dev/raw/robot_logs/ingest_date=2030-01-02/SYNTHETIC_sample.xlsx"
    assert r.staged_key == "dev/staged/robot_logs/ingest_date=2030-01-02/SYNTHETIC_sample.csv"
    original = s3.get_object(Bucket=BUCKET, Key=r.raw_key)["Body"].read()
    assert original == sample_file.read_bytes()  # byte-for-byte unchanged


def test_staged_csv_keeps_every_value(s3, sample_file):
    r = cloud_s3.upload_file(sample_file, BUCKET, DAY, client=s3)
    body = s3.get_object(Bucket=BUCKET, Key=r.staged_key)["Body"].read()
    csv = pd.read_csv(io.BytesIO(body), dtype={"Timestamp": str})
    src = pd.read_excel(sample_file)
    assert list(csv.columns) == [*src.columns, "source_file"]
    assert (csv["Timestamp"] == src["Timestamp"]).all()  # stray quotes survive
    assert csv["Robot_ProtectiveStop"].isna().sum() == src["Robot_ProtectiveStop"].isna().sum()
    assert (csv["source_file"] == "SYNTHETIC_sample.xlsx").all()


def test_second_upload_of_same_content_is_skipped(s3, sample_file):
    cloud_s3.upload_file(sample_file, BUCKET, DAY, client=s3)
    assert not cloud_s3.upload_file(sample_file, BUCKET, DAY, client=s3).uploaded


def test_changed_content_is_uploaded_again(s3, tmp_path):
    path = write_sample(tmp_path / "SYNTHETIC_day.xlsx", rows=20, seed=1)
    cloud_s3.upload_file(path, BUCKET, DAY, client=s3)
    write_sample(path, rows=25, seed=2)
    assert cloud_s3.upload_file(path, BUCKET, DAY, client=s3).uploaded


def test_invalid_file_is_never_uploaded(s3, tmp_path):
    bad = tmp_path / "SYNTHETIC_bad.xlsx"
    make_frame(rows=5).drop(columns=["grip_lost"]).to_excel(bad, index=False)
    with pytest.raises(LoaderError):
        cloud_s3.upload_file(bad, BUCKET, DAY, client=s3)
    assert s3.list_objects_v2(Bucket=BUCKET).get("KeyCount", 0) == 0


def test_download_returns_the_same_bytes(s3, sample_file, tmp_path):
    cloud_s3.upload_file(sample_file, BUCKET, DAY, client=s3)
    [path] = cloud_s3.download_raw(BUCKET, DAY, tmp_path / "back", client=s3)
    assert path.read_bytes() == sample_file.read_bytes()


def test_missing_bucket_setting_fails_clearly(monkeypatch):
    monkeypatch.delenv("ROBOT_S3_BUCKET", raising=False)
    with pytest.raises(SystemExit, match="ROBOT_S3_BUCKET"):
        cloud_s3.bucket_name()
