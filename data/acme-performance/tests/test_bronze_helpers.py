"""Unit tests for configuration, Bronze helpers, retries and reconciliation."""

from __future__ import annotations

import pandas as pd
import psycopg2
import pytest

import job

BASE_ENV = {"POSTGRES_HOST": "localhost", "POSTGRES_NAME": "db", "POSTGRES_USER": "u",
            "DATA_LAKE_BUCKET": "coding-workshop-datalake-test"}


def make_settings(**overrides):
    values = dict(is_local=True, pg_host="h", pg_port=5432, pg_name="db", pg_user="u", pg_pass="p",
                  bucket="b", aws_region="ap-south-1", s3_endpoint=None)
    values.update(overrides)
    return job.Settings(**values)


def test_settings_from_env(monkeypatch):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("IS_LOCAL", "false")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    settings = job.Settings.from_env()
    assert settings.is_local is False and settings.pg_port == 6543
    assert settings.bucket == "coding-workshop-datalake-test" and settings.s3_endpoint is None


@pytest.mark.parametrize(
    ("is_local", "override", "expected"),
    [(True, None, "prefer"), (False, None, "require"), (False, "disable", "disable")],
)
def test_connect_postgres_sslmode(monkeypatch, is_local, override, expected):
    captured = {}

    class FakeConn:
        def set_session(self, readonly):
            captured["readonly"] = readonly

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeConn()

    monkeypatch.setattr(job.psycopg2, "connect", fake_connect)
    job.connect_postgres(make_settings(is_local=is_local, pg_sslmode=override))
    assert captured["sslmode"] == expected and captured["readonly"] is True


def test_settings_sslmode_from_env(monkeypatch):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("POSTGRES_SSLMODE", "disable")
    assert job.Settings.from_env().pg_sslmode == "disable"
    monkeypatch.delenv("POSTGRES_SSLMODE")
    assert job.Settings.from_env().pg_sslmode is None


def test_settings_bucket_falls_back_to_participant_id(monkeypatch):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATA_LAKE_BUCKET")
    monkeypatch.delenv("APP_ID", raising=False)
    monkeypatch.setenv("PARTICIPANT_ID", "abcd1234")
    assert job.Settings.from_env().bucket == "coding-workshop-datalake-abcd1234"


def test_settings_missing_variables_are_named(monkeypatch):
    for key in [*BASE_ENV, "APP_ID", "PARTICIPANT_ID"]:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match="POSTGRES_HOST"):
        job.Settings.from_env()


def test_run_context_ingest_date_is_utc_date(run_context):
    assert run_context.ingest_date == run_context.started_at.strftime("%Y-%m-%d")
    assert run_context.run_id.startswith(run_context.started_at.strftime("%Y%m%dT"))


def test_bronze_keys_keep_partitions():
    settings = make_settings()
    assert job.bronze_table_key(settings, "teams", "2026-10-14") == \
        "bronze/postgres/teams/ingest_date=2026-10-14/teams.parquet"
    landing = "landing/learning_activity/year=2025/month=01/day=10/events-000.jsonl"
    assert job.bronze_event_key(settings, landing) == \
        "bronze/learning_activity/year=2025/month=01/day=10/events-000.jsonl"


@pytest.mark.parametrize(("path", "valid"), [
    ("year=2025/month=01/day=10/events-000.jsonl", True),
    ("year=25/month=1/day=10/events-000.jsonl", False),
    ("events-000.jsonl", False),
    ("year=2025/month=01/day=10/events.csv", False),
])
def test_partition_pattern(path, valid):
    assert bool(job.PARTITION_PATTERN.fullmatch(path)) is valid


def test_ingestion_metadata_added_without_changing_source_columns(run_context):
    source = pd.DataFrame({"team_id": [1, 2], "name": ["A", "B"]})
    out = job.add_ingestion_metadata(source, run_context, "postgres", "db.public.teams")
    assert list(out.columns[:2]) == ["team_id", "name"]
    assert set(out["_source_system"]) == {"postgres"} and set(out["_ingest_run_id"]) == {run_context.run_id}
    assert list(source.columns) == ["team_id", "name"]  # input not mutated


def test_extract_table_rejects_unknown_tables():
    with pytest.raises(ValueError, match="Unknown source table"):
        job.extract_table(conn=None, table="pg_user; DROP TABLE x")


def test_retry_succeeds_after_transient_errors(monkeypatch):
    sleeps = []
    monkeypatch.setattr(job.time, "sleep", sleeps.append)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise psycopg2.OperationalError("connection timeout")
        return "ok"

    assert job.with_retry(flaky, "postgres", retry_on=(psycopg2.OperationalError,)) == "ok"
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_retry_gives_up_and_reraises(monkeypatch):
    monkeypatch.setattr(job.time, "sleep", lambda _: None)

    def always_fails():
        raise psycopg2.OperationalError("down")

    with pytest.raises(psycopg2.OperationalError):
        job.with_retry(always_fails, "postgres", attempts=3, retry_on=(psycopg2.OperationalError,))


def test_retry_does_not_retry_unexpected_errors(monkeypatch):
    monkeypatch.setattr(job.time, "sleep", lambda _: pytest.fail("should not sleep"))
    with pytest.raises(KeyError):
        job.with_retry(lambda: {}["x"], "x", retry_on=(psycopg2.OperationalError,))


def test_entity_reconciliation():
    assert job.EntityResult("e", rows_read=10, duplicates_removed=2, rows_rejected=3, rows_written=5).reconciled
    assert not job.EntityResult("e", rows_read=10, duplicates_removed=0, rows_rejected=0, rows_written=9).reconciled
    with pytest.raises(RuntimeError, match="Reconciliation failed"):
        job.log_entity(job.EntityResult("e", 10, 0, 0, 9))


def test_json_log_format():
    import io
    import json

    stream = io.StringIO()
    handler = job.logging.StreamHandler(stream)
    handler.setFormatter(job.JsonFormatter())
    job.LOG.addHandler(handler)
    try:
        job.log(job.logging.INFO, "hello", rows=3)
    finally:
        job.LOG.removeHandler(handler)
    record = json.loads(stream.getvalue().strip().splitlines()[-1])
    assert record["msg"] == "hello" and record["rows"] == 3 and record["level"] == "INFO"
