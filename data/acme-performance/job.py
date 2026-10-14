"""ACME Inc. employee performance data pipeline (Bronze -> Silver -> Gold).

Batch ETL job for the Citi Data Engineering Workshop. It follows the workshop's
``data/<job>/job.py`` convention so the same file runs locally and on EKS.

Layers (all in one S3 data-lake bucket):
    landing/  files dropped by source systems (learning-activity JSONL events)
    bronze/   raw, immutable copies of every source plus ingestion metadata
    silver/   cleaned, validated, conformed data           (next step)
    gold/     business-ready analytics datasets            (later step)

Configuration comes only from environment variables (see ``Settings``):
    IS_LOCAL, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME, POSTGRES_USER,
    POSTGRES_PASS, DATA_LAKE_BUCKET, AWS_REGION, AWS_ENDPOINT_URL (LocalStack)

Usage:
    python job.py                 # run every implemented stage
    python job.py --stage bronze  # run one stage
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import boto3
import pandas as pd
import psycopg2
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

T = TypeVar("T")

# Source tables in foreign-key order, as defined by the dataset's schema.sql.
SOURCE_TABLES: tuple[str, ...] = (
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)

# Landing files must sit in Hive-style date partitions.
PARTITION_PATTERN = re.compile(r"year=\d{4}/month=\d{2}/day=\d{2}/[^/]+\.jsonl$")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line (structured logging)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger() -> logging.Logger:
    """Return the job logger, configured once with the JSON formatter."""
    logger = logging.getLogger("acme-performance")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger


LOG = get_logger()


def log(level: int, msg: str, **fields: Any) -> None:
    """Log ``msg`` with extra structured ``fields``."""
    LOG.log(level, msg, extra={"fields": fields})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    """Runtime configuration, read from environment variables only."""

    is_local: bool
    pg_host: str
    pg_port: int
    pg_name: str
    pg_user: str
    pg_pass: str
    bucket: str
    aws_region: str
    s3_endpoint: str | None
    landing_prefix: str = "landing/learning_activity/"
    bronze_prefix: str = "bronze/"

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from the workshop's standard environment variables."""
        app_id = os.getenv("APP_ID") or os.getenv("PARTICIPANT_ID") or ""
        bucket = os.getenv("DATA_LAKE_BUCKET") or (
            f"coding-workshop-datalake-{app_id}" if app_id else ""
        )
        missing = [
            name
            for name, value in {
                "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
                "POSTGRES_NAME": os.getenv("POSTGRES_NAME"),
                "POSTGRES_USER": os.getenv("POSTGRES_USER"),
                "DATA_LAKE_BUCKET (or APP_ID/PARTICIPANT_ID)": bucket,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        return cls(
            is_local=os.getenv("IS_LOCAL", "true").lower() == "true",
            pg_host=os.environ["POSTGRES_HOST"],
            pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
            pg_name=os.environ["POSTGRES_NAME"],
            pg_user=os.environ["POSTGRES_USER"],
            pg_pass=os.getenv("POSTGRES_PASS", ""),
            bucket=bucket,
            aws_region=os.getenv("AWS_REGION", "ap-south-1"),
            s3_endpoint=os.getenv("AWS_ENDPOINT_URL") or None,
        )


@dataclass(frozen=True)
class RunContext:
    """Identifiers shared by every record written in one job run."""

    run_id: str
    started_at: datetime

    @property
    def ingest_date(self) -> str:
        """Partition value for this run (UTC date)."""
        return self.started_at.strftime("%Y-%m-%d")

    @classmethod
    def new(cls) -> "RunContext":
        """Create a context for a new run."""
        now = datetime.now(timezone.utc)
        return cls(run_id=f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}", started_at=now)


# ---------------------------------------------------------------------------
# Connections with retry
# ---------------------------------------------------------------------------
def with_retry(
    action: Callable[[], T],
    what: str,
    attempts: int = 4,
    base_delay: float = 1.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Run ``action``; retry with exponential backoff on transient errors."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except retry_on as exc:
            if attempt == attempts:
                log(logging.ERROR, "giving up after retries", target=what, attempts=attempts, error=str(exc))
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log(logging.WARNING, "transient failure, retrying", target=what, attempt=attempt, retry_in_s=delay, error=str(exc))
            time.sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover


def connect_postgres(settings: Settings) -> "psycopg2.extensions.connection":
    """Open a read-only PostgreSQL connection (SSL required in the cloud)."""

    def _connect() -> "psycopg2.extensions.connection":
        conn = psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.pg_name,
            user=settings.pg_user,
            password=settings.pg_pass,
            sslmode="prefer" if settings.is_local else "require",
            connect_timeout=10,
        )
        conn.set_session(readonly=True)
        return conn

    return with_retry(_connect, "postgres", retry_on=(psycopg2.OperationalError,))


def s3_client(settings: Settings) -> Any:
    """Create an S3 client; path-style addressing when using LocalStack."""
    config = Config(
        retries={"max_attempts": 5, "mode": "standard"},
        s3={"addressing_style": "path"} if settings.s3_endpoint else {},
    )
    return boto3.client(
        "s3", region_name=settings.aws_region, endpoint_url=settings.s3_endpoint, config=config
    )


# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------
def bronze_table_key(settings: Settings, table: str, ingest_date: str) -> str:
    """S3 key of the Bronze snapshot for one table and one ingest date."""
    return f"{settings.bronze_prefix}postgres/{table}/ingest_date={ingest_date}/{table}.parquet"


def bronze_event_key(settings: Settings, landing_key: str) -> str:
    """Map a landing key to its Bronze key, keeping the date partitions."""
    relative = landing_key[len(settings.landing_prefix):]
    return f"{settings.bronze_prefix}learning_activity/{relative}"


def add_ingestion_metadata(df: pd.DataFrame, run: RunContext, source_system: str, source_object: str) -> pd.DataFrame:
    """Return a copy of ``df`` with Bronze ingestion metadata columns."""
    out = df.copy()
    out["_ingest_run_id"] = run.run_id
    out["_ingested_at"] = run.started_at.isoformat()
    out["_source_system"] = source_system
    out["_source_object"] = source_object
    return out


def extract_table(conn: Any, table: str) -> pd.DataFrame:
    """Read a whole source table exactly as stored (no transformation)."""
    if table not in SOURCE_TABLES:
        raise ValueError(f"Unknown source table: {table}")
    with conn.cursor() as cur:
        # The table name is checked against SOURCE_TABLES above, so this is not injectable.
        cur.execute(f"SELECT * FROM {table}")  # nosec B608
        columns = [col.name for col in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def ingest_postgres_table(conn: Any, s3: Any, settings: Settings, run: RunContext, table: str) -> dict[str, Any]:
    """Snapshot one PostgreSQL table into Bronze as Parquet.

    The key is fixed per ingest date, so re-running on the same day overwrites
    the same object instead of adding duplicates (idempotent).
    """
    df = extract_table(conn, table)
    if df.empty:
        log(logging.WARNING, "source table is empty", table=table)
    bronze = add_ingestion_metadata(df, run, "postgres", f"{settings.pg_name}.public.{table}")
    buffer = io.BytesIO()
    bronze.to_parquet(buffer, index=False)
    body = buffer.getvalue()
    key = bronze_table_key(settings, table, run.ingest_date)
    with_retry(
        lambda: s3.put_object(Bucket=settings.bucket, Key=key, Body=body),
        f"s3://{settings.bucket}/{key}",
        retry_on=(BotoCoreError, ClientError),
    )
    record = {
        "source": f"postgres://{settings.pg_host}/{settings.pg_name}/public.{table}",
        "target": f"s3://{settings.bucket}/{key}",
        "rows_read": len(df),
        "rows_written": len(bronze),
        "bytes_written": len(body),
        "md5": hashlib.md5(body, usedforsecurity=False).hexdigest(),
    }
    log(logging.INFO, "bronze table written", table=table, rows=len(df), target=record["target"])
    return record


def list_keys(s3: Any, bucket: str, prefix: str) -> dict[str, dict[str, Any]]:
    """Return ``{key: {"size": int, "etag": str}}`` for every object under ``prefix``."""
    found: dict[str, dict[str, Any]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            found[obj["Key"]] = {"size": obj["Size"], "etag": obj["ETag"]}
    return found


def ingest_landing_events(s3: Any, settings: Settings, run: RunContext) -> list[dict[str, Any]]:
    """Copy landing JSONL files into Bronze byte-for-byte (immutable, incremental).

    - Files keep their ``year=/month=/day=`` partitions.
    - A file already in Bronze with the same size is skipped, so reruns never
      duplicate data and only new files are copied.
    - Files outside the partition layout are logged and left in landing.
    """
    landing = list_keys(s3, settings.bucket, settings.landing_prefix)
    bronze = list_keys(s3, settings.bucket, f"{settings.bronze_prefix}learning_activity/")
    if not landing:
        log(logging.WARNING, "no landing files found", prefix=settings.landing_prefix)

    records: list[dict[str, Any]] = []
    copied = skipped = rejected = 0
    for key, meta in sorted(landing.items()):
        relative = key[len(settings.landing_prefix):]
        if not PARTITION_PATTERN.fullmatch(relative):
            rejected += 1
            log(logging.WARNING, "landing file outside partition layout, not ingested", key=key)
            continue
        target = bronze_event_key(settings, key)
        if target in bronze and bronze[target]["size"] == meta["size"]:
            skipped += 1
            continue
        with_retry(
            lambda: s3.copy_object(
                Bucket=settings.bucket,
                Key=target,
                CopySource={"Bucket": settings.bucket, "Key": key},
                MetadataDirective="REPLACE",
                ContentType="application/x-ndjson",
                Metadata={
                    "ingest-run-id": run.run_id,
                    "ingested-at": run.started_at.isoformat(),
                    "source-system": "s3-landing",
                    "source-key": key,
                },
            ),
            f"s3://{settings.bucket}/{target}",
            retry_on=(BotoCoreError, ClientError),
        )
        written = s3.head_object(Bucket=settings.bucket, Key=target)["ContentLength"]
        if written != meta["size"]:
            raise RuntimeError(f"Size mismatch after copy: {key} ({meta['size']}) -> {target} ({written})")
        copied += 1
        records.append(
            {
                "source": f"s3://{settings.bucket}/{key}",
                "target": f"s3://{settings.bucket}/{target}",
                "bytes_read": meta["size"],
                "bytes_written": written,
            }
        )
    log(
        logging.INFO,
        "bronze learning_activity ingested",
        landing_files=len(landing),
        copied=copied,
        skipped_already_in_bronze=skipped,
        rejected=rejected,
    )
    return records


def write_manifest(s3: Any, settings: Settings, run: RunContext, stage: str, status: str, records: list[dict[str, Any]]) -> str:
    """Write a lineage manifest describing every source->target move in this run."""
    key = f"{settings.bronze_prefix}_lineage/ingest_date={run.ingest_date}/{stage}-{run.run_id}.json"
    manifest = {
        "run_id": run.run_id,
        "stage": stage,
        "status": status,
        "started_at": run.started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }
    s3.put_object(
        Bucket=settings.bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2, default=str).encode(),
        ContentType="application/json",
    )
    return f"s3://{settings.bucket}/{key}"


def ingest_into_bronze(settings: Settings, run: RunContext) -> dict[str, Any]:
    """Ingest every source (PostgreSQL tables + S3 landing files) into Bronze."""
    s3 = s3_client(settings)
    records: list[dict[str, Any]] = []
    status = "FAILED"
    try:
        conn = connect_postgres(settings)
        try:
            for table in SOURCE_TABLES:
                records.append(ingest_postgres_table(conn, s3, settings, run, table))
        finally:
            conn.close()
        records.extend(ingest_landing_events(s3, settings, run))
        status = "SUCCEEDED"
    finally:
        manifest = write_manifest(s3, settings, run, "bronze", status, records)
        log(logging.INFO, "lineage manifest written", manifest=manifest, status=status)
    return {
        "tables": sum(1 for r in records if r["source"].startswith("postgres://")),
        "table_rows": sum(r.get("rows_written", 0) for r in records),
        "event_files_copied": sum(1 for r in records if r["source"].startswith("s3://")),
        "manifest": manifest,
    }


# ---------------------------------------------------------------------------
# Silver and Gold layers (next steps)
# ---------------------------------------------------------------------------
def transform_to_silver(settings: Settings, run: RunContext) -> dict[str, Any]:
    """Transform Bronze data into the Silver layer (not implemented yet)."""
    log(logging.WARNING, "silver stage not implemented yet - skipped")
    return {"status": "SKIPPED"}


def aggregate_to_gold(settings: Settings, run: RunContext) -> dict[str, Any]:
    """Aggregate Silver data into the Gold layer (not implemented yet)."""
    log(logging.WARNING, "gold stage not implemented yet - skipped")
    return {"status": "SKIPPED"}


STAGES: dict[str, Callable[[Settings, RunContext], dict[str, Any]]] = {
    "bronze": ingest_into_bronze,
    "silver": transform_to_silver,
    "gold": aggregate_to_gold,
}


def main(argv: list[str] | None = None) -> int:
    """Run the requested pipeline stages in order; return a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stage", choices=[*STAGES, "all"], default="all")
    args = parser.parse_args(argv)

    run = RunContext.new()
    stages = list(STAGES) if args.stage == "all" else [args.stage]
    log(logging.INFO, "job started", run_id=run.run_id, stages=stages)
    status, exit_code = "SUCCEEDED", 0
    try:
        settings = Settings.from_env()
        log(logging.INFO, "configuration loaded", is_local=settings.is_local, bucket=settings.bucket,
            postgres=f"{settings.pg_host}:{settings.pg_port}/{settings.pg_name}",
            s3_endpoint=settings.s3_endpoint or "aws")
        for name in stages:
            started = time.monotonic()
            log(logging.INFO, "stage started", stage=name)
            result = STAGES[name](settings, run)
            log(logging.INFO, "stage finished", stage=name, duration_s=round(time.monotonic() - started, 2), **result)
    except Exception:  # noqa: BLE001 - top-level guard: log and exit non-zero
        status, exit_code = "FAILED", 1
        LOG.exception("job failed", extra={"fields": {"run_id": run.run_id}})
    duration = (datetime.now(timezone.utc) - run.started_at).total_seconds()
    log(logging.INFO, "job finished", run_id=run.run_id, status=status, duration_s=round(duration, 2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
