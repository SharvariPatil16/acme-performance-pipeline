"""Integration test: Bronze -> Silver -> Gold end to end, then re-run (idempotency).

Reads the real PostgreSQL source configured by the POSTGRES_* variables and uses an
in-memory S3 (moto), seeded with the sample dataset's landing files, so it never
touches LocalStack or AWS buckets. Skipped when PostgreSQL or the dataset is missing.

    ACME_DATASET_DIR  folder containing learning_activity/ (default: repo sampleData/)
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
import pytest

import job
from conftest import REPO_ROOT

pytestmark = pytest.mark.integration

DATASET = Path(os.getenv("ACME_DATASET_DIR",
                         REPO_ROOT / "sampleData" / "data-engineering-performance-talent-management"))
BUCKET = "coding-workshop-datalake-itest"


def postgres_counts() -> dict[str, int]:
    """Row count of every source table, straight from PostgreSQL."""
    conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=int(os.getenv("POSTGRES_PORT", "5432")),
                            dbname=os.environ["POSTGRES_NAME"], user=os.environ["POSTGRES_USER"],
                            password=os.getenv("POSTGRES_PASS", ""), connect_timeout=5)
    try:
        with conn.cursor() as cur:
            counts = {}
            for table in job.SOURCE_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table}")  # nosec B608 - allow-listed names
                counts[table] = cur.fetchone()[0]
            return counts
    finally:
        conn.close()


@pytest.fixture
def lake(monkeypatch):
    """In-memory S3 bucket seeded with the landing files; yields an S3 client."""
    moto = pytest.importorskip("moto")
    if not os.getenv("POSTGRES_HOST"):
        pytest.skip("POSTGRES_* not set")
    try:
        expected = postgres_counts()
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    if not (DATASET / "learning_activity").is_dir():
        pytest.skip(f"dataset not found at {DATASET}")

    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("DATA_LAKE_BUCKET", BUCKET)
    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        for path in sorted((DATASET / "learning_activity").rglob("*.jsonl")):
            key = "landing/learning_activity/" + path.relative_to(DATASET / "learning_activity").as_posix()
            s3.upload_file(str(path), BUCKET, key)
        yield s3, expected


def keys(s3, prefix: str) -> dict[str, int]:
    """``{key: size}`` for every object under ``prefix``."""
    return {k: v["size"] for k, v in job.list_keys(s3, BUCKET, prefix).items()}


def latest_manifest(s3, layer: str) -> dict:
    manifest_keys = sorted(keys(s3, f"{layer}/_lineage/"))
    return json.loads(s3.get_object(Bucket=BUCKET, Key=manifest_keys[-1])["Body"].read())


def read_parquet(s3, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))


def data_keys(s3, prefix: str) -> set[str]:
    """Data files only (lineage manifests are expected to grow on every run)."""
    return {k for k in keys(s3, prefix) if "/_lineage/" not in k}


def test_pipeline_end_to_end_and_rerun_is_idempotent(lake):
    s3, expected = lake
    landing = keys(s3, "landing/learning_activity/")
    landing_lines = sum(
        1 for path in (DATASET / "learning_activity").rglob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines() if line.strip())

    # ---- Bronze: every row and every file arrives unchanged --------------------------
    assert job.main(["--stage", "bronze"]) == 0
    for table, count in expected.items():
        [key] = [k for k in keys(s3, f"bronze/postgres/{table}/") if k.endswith(".parquet")]
        bronze = read_parquet(s3, key)
        assert len(bronze) == count, table
        assert {"_ingest_run_id", "_ingested_at", "_source_system", "_source_object"} <= set(bronze.columns)
    bronze_files = keys(s3, "bronze/learning_activity/")
    assert len(bronze_files) == len(landing)
    for landing_key, size in landing.items():  # file sizes match the source export
        assert bronze_files[landing_key.replace("landing/", "bronze/", 1)] == size
    assert latest_manifest(s3, "bronze")["status"] == "SUCCEEDED"

    bronze_before = data_keys(s3, "bronze/")
    assert job.main(["--stage", "bronze"]) == 0                       # re-run
    assert data_keys(s3, "bronze/") == bronze_before                 # nothing new
    assert not [r for r in latest_manifest(s3, "bronze")["records"] if r["source"].startswith("s3://")]

    # ---- Silver: everything reconciles; bad events are quarantined -------------------
    assert job.main(["--stage", "silver"]) == 0
    silver = latest_manifest(s3, "silver")
    assert silver["status"] == "SUCCEEDED"
    results = {r["entity"]: r for r in silver["records"]}
    assert all(r["reconciled"] for r in results.values())
    for table, count in expected.items():
        assert results[table]["rows_read"] == count
    events = results["learning_activity"]
    assert events["rows_read"] == landing_lines
    assert events["duplicates_removed"] > 0 and events["rows_rejected"] > 0
    quarantine = [k for k in keys(s3, "silver/_quarantine/learning_activity/")]
    assert quarantine, "rejected events must be written to quarantine"
    reasons = {json.loads(line)["reason"] for line in
               s3.get_object(Bucket=BUCKET, Key=quarantine[0])["Body"].read().decode().splitlines()}
    assert {"malformed_json", "schema_validation"} <= reasons

    silver_before = data_keys(s3, "silver/")
    written_before = {e: r["rows_written"] for e, r in results.items()}
    assert job.main(["--stage", "silver"]) == 0                       # re-run
    assert {r["entity"]: r["rows_written"] for r in latest_manifest(s3, "silver")["records"]} == written_before
    assert len(data_keys(s3, "silver/")) == len(silver_before)       # same number of files, no duplicates

    # ---- Gold: every dataset passes its grain and reconciliation checks --------------
    assert job.main(["--stage", "gold"]) == 0
    gold = latest_manifest(s3, "gold")
    assert gold["status"] == "SUCCEEDED"
    tables = {r["target"]: r for r in gold["records"] if "target" in r}
    assert set(tables) == {f"gold/{t.name}" for t in job.GOLD_TABLES}
    assert all(t["grain_unique"] and t["rows_match_expected"] and t["null_grain_keys"] == 0 for t in tables.values())
    assert tables["gold/promotion_readiness"]["rows"] == expected["employees"]
    events_check = next(r for r in gold["records"] if r.get("check") == "events_silver_vs_gold")
    assert events_check["silver"] == events_check["gold"] == events["rows_written"]


def test_seed_loads_empty_database_once(lake, monkeypatch):
    """The cloud bootstrap stage loads an empty database, then never touches it again."""
    s3, expected = lake
    for path in [DATASET / "schema.sql", *(DATASET / f"{t}.csv" for t in job.SOURCE_TABLES)]:
        s3.upload_file(str(path), BUCKET, f"seed/postgres/{path.name}")
    admin = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=int(os.getenv("POSTGRES_PORT", "5432")),
                             dbname=os.environ["POSTGRES_NAME"], user=os.environ["POSTGRES_USER"],
                             password=os.getenv("POSTGRES_PASS", ""))
    admin.autocommit = True
    scratch_db = "acme_seed_itest"
    try:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {scratch_db}")
            cur.execute(f"CREATE DATABASE {scratch_db}")
    except psycopg2.Error as exc:
        admin.close()
        pytest.skip(f"cannot create a scratch database: {exc}")
    try:
        monkeypatch.setenv("POSTGRES_NAME", scratch_db)
        monkeypatch.setenv("SOURCE_SEED_PREFIX", "seed/postgres/")
        assert job.main(["--stage", "seed"]) == 0
        assert postgres_counts() == expected                     # every table fully loaded
        assert job.main(["--stage", "seed"]) == 0                 # second run: already loaded
        assert postgres_counts() == expected                     # no duplicates
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {scratch_db} WITH (FORCE)")
        admin.close()


def test_seed_is_skipped_without_prefix(monkeypatch):
    monkeypatch.delenv("SOURCE_SEED_PREFIX", raising=False)
    assert job.seed_source_database(settings=None, run=None) == {"status": "SKIPPED"}


def test_failed_run_is_logged_and_exits_non_zero(lake, monkeypatch):
    """Unreachable database: the job retries, records a FAILED manifest and exits 1."""
    s3, _ = lake
    monkeypatch.setenv("POSTGRES_PORT", "1")          # nothing listens on port 1
    monkeypatch.setattr(job.time, "sleep", lambda _: None)
    assert job.main(["--stage", "bronze"]) == 1
    assert latest_manifest(s3, "bronze")["status"] == "FAILED"
