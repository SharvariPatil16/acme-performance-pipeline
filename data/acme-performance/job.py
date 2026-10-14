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
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal, Optional, TypeVar

import boto3
import pandas as pd
import psycopg2
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

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
# Silver layer: schema contracts (Pydantic)
# ---------------------------------------------------------------------------
# Each model is the Silver "contract" for one entity. Rules come from the
# dataset's schema.sql (NOT NULL, CHECK constraints) plus basic domain sense.
class SilverModel(BaseModel):
    """Base contract: trim strings, ignore Bronze metadata columns, immutable."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True, frozen=True)


NonEmpty = Field(min_length=1)
PositiveId = Field(gt=0)
Level = Field(ge=1, le=5)


class Team(SilverModel):
    team_id: int = PositiveId
    name: str = NonEmpty
    department: str = NonEmpty


class Competency(SilverModel):
    competency_id: int = PositiveId
    name: str = NonEmpty
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(SilverModel):
    employee_id: int = PositiveId
    first_name: str = NonEmpty
    last_name: str = NonEmpty
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    team_id: int = PositiveId
    job_level: str = NonEmpty
    hire_date: date

    @field_validator("email")
    @classmethod
    def lowercase_email(cls, value: str) -> str:
        """Store emails in one canonical (lower-case) form."""
        return value.lower()

    @field_validator("hire_date")
    @classmethod
    def not_in_future(cls, value: date) -> date:
        """A hire date cannot be after today."""
        if value > datetime.now(timezone.utc).date():
            raise ValueError("hire_date is in the future")
        return value


class Project(SilverModel):
    project_id: int = PositiveId
    name: str = NonEmpty
    team_id: int = PositiveId
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: Decimal = Field(ge=0, le=100, max_digits=5, decimal_places=2)


class PerformanceReview(SilverModel):
    review_id: int = PositiveId
    employee_id: int = PositiveId
    review_period: str = Field(pattern=r"^\d{4}-H[12]$")
    rating: int = Level
    manager_comment: Optional[str] = None
    submitted_date: date


class CompetencyAssessment(SilverModel):
    assessment_id: int = PositiveId
    employee_id: int = PositiveId
    competency_id: int = PositiveId
    proficiency_level: int = Level
    required_level: int = Level
    assessed_date: date


class TalentReview(SilverModel):
    employee_id: int = PositiveId
    review_cycle: str = Field(pattern=r"^\d{4}-H[12]$")
    potential_rating: Literal["Low", "Medium", "High"]
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: Literal["Low", "Medium", "High"]


class LearningEvent(SilverModel):
    event_id: str = Field(pattern=r"^EV\d{10}$")
    employee_id: int = PositiveId
    event_date: datetime
    event_type: Literal[
        "enrollment", "module_started", "module_completed", "quiz_attempted",
        "quiz_passed", "course_completed", "certification_earned",
    ]
    course_name: str = NonEmpty
    text: Optional[str] = None
    score: Optional[int] = Field(default=None, ge=0, le=100)

    @field_validator("event_date")
    @classmethod
    def as_utc(cls, value: datetime) -> datetime:
        """Source timestamps have no zone; the data lake stores them as UTC."""
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

    @model_validator(mode="before")
    @classmethod
    def conform_raw_event(cls, data: Any) -> Any:
        """Conform schema drift: some producers send ``notes`` instead of ``text``."""
        if isinstance(data, dict) and data.get("text") is None and "notes" in data:
            data = {**data, "text": data.get("notes")}
        return data


@dataclass(frozen=True)
class ForeignKey:
    """``column`` must exist as ``parent_key`` in the Silver ``parent`` entity."""

    column: str
    parent: str
    parent_key: str


@dataclass(frozen=True)
class EntitySpec:
    """How one Silver entity is validated and stored."""

    name: str
    model: type[SilverModel]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()
    partition_by: tuple[str, ...] = field(default=())


# Processing order matters: parents before children (foreign-key checks).
TABLE_SPECS: tuple[EntitySpec, ...] = (
    EntitySpec("teams", Team, ("team_id",)),
    EntitySpec("competencies", Competency, ("competency_id",)),
    EntitySpec("employees", Employee, ("employee_id",), (ForeignKey("team_id", "teams", "team_id"),)),
    EntitySpec("projects", Project, ("project_id",), (ForeignKey("team_id", "teams", "team_id"),)),
    EntitySpec("performance_reviews", PerformanceReview, ("review_id",),
               (ForeignKey("employee_id", "employees", "employee_id"),)),
    EntitySpec("competency_assessments", CompetencyAssessment, ("assessment_id",),
               (ForeignKey("employee_id", "employees", "employee_id"),
                ForeignKey("competency_id", "competencies", "competency_id"))),
    EntitySpec("talent_review", TalentReview, ("employee_id",),
               (ForeignKey("employee_id", "employees", "employee_id"),)),
)
EVENT_SPEC = EntitySpec(
    "learning_activity", LearningEvent, ("event_id",),
    (ForeignKey("employee_id", "employees", "employee_id"),),
    partition_by=("event_year", "event_month"),
)


# ---------------------------------------------------------------------------
# Silver layer: pure validation helpers (unit-testable without Spark)
# ---------------------------------------------------------------------------
def validate_record(raw: dict[str, Any], model: type[SilverModel]) -> tuple[bool, dict[str, Any]]:
    """Validate one record against ``model``.

    Returns ``(True, clean_dict)`` or ``(False, rejection)`` where the rejection
    keeps the raw record and every field error so it can be quarantined.
    """
    try:
        return True, model.model_validate(raw).model_dump()
    except ValidationError as exc:
        errors = [f"{'.'.join(str(p) for p in err['loc']) or 'record'}: {err['msg']}" for err in exc.errors()]
        return False, {"reason": "schema_validation", "errors": errors, "record": raw}


def parse_event_line(line: str) -> tuple[bool, dict[str, Any]]:
    """Parse one raw JSONL line; malformed JSON becomes a rejection."""
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        return False, {"reason": "malformed_json", "errors": [str(exc)], "record": {"raw_line": line}}
    if not isinstance(value, dict):
        return False, {"reason": "malformed_json", "errors": ["line is not a JSON object"], "record": {"raw_line": line}}
    return True, value


def validate_event_line(line: str, source_file: str) -> tuple[bool, dict[str, Any]]:
    """Parse and validate one learning-activity line, keeping its source file."""
    ok, parsed = parse_event_line(line)
    if ok:
        ok, parsed = validate_record(parsed, LearningEvent)
    parsed["_source_file"] = source_file
    return ok, parsed


# ---------------------------------------------------------------------------
# Silver layer: Spark + S3 plumbing
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str = "acme-performance") -> Any:
    """Create the Spark session (local mode unless SPARK_MASTER is set)."""
    from pyspark.sql import SparkSession  # imported lazily: Bronze does not need Spark

    # Spark's Python workers must use this interpreter (and its packages, e.g.
    # pydantic), not whatever "python3" happens to be first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def spark_schema(model: type[SilverModel], extra: Iterable[tuple[str, Any]] = ()) -> Any:
    """Build the explicit Spark schema for a Silver entity from its contract."""
    from pyspark.sql import types as T

    mapping = {int: T.LongType(), str: T.StringType(), date: T.DateType(),
               datetime: T.TimestampType(), Decimal: T.DecimalType(5, 2)}
    fields = []
    for name, info in model.model_fields.items():
        annotation = info.annotation
        args = [a for a in getattr(annotation, "__args__", ()) if a is not type(None)]
        base = args[0] if getattr(annotation, "__origin__", None) is not Literal and args else annotation
        if getattr(annotation, "__origin__", None) is Literal:
            base = str
        fields.append(T.StructField(name, mapping[base], nullable=not info.is_required()))
    fields += [T.StructField(name, dtype, True) for name, dtype in extra]
    return T.StructType(fields)


def download_prefix(s3: Any, bucket: str, prefix: str, target: Path) -> int:
    """Download every object under ``prefix`` into ``target`` keeping relative paths."""
    count = 0
    for key in list_keys(s3, bucket, prefix):
        dest = target / key[len(prefix):]
        dest.parent.mkdir(parents=True, exist_ok=True)
        with_retry(lambda: s3.download_file(bucket, key, str(dest)), f"s3://{bucket}/{key}",
                   retry_on=(BotoCoreError, ClientError))
        count += 1
    return count


def replace_prefix(s3: Any, bucket: str, prefix: str, source_dir: Path) -> int:
    """Make ``prefix`` contain exactly the files in ``source_dir`` (idempotent overwrite)."""
    stale = list(list_keys(s3, bucket, prefix))
    for start in range(0, len(stale), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in stale[start:start + 1000]]})
    uploaded = 0
    for path in sorted(source_dir.rglob("*")):
        if path.is_file() and not path.name.endswith(".crc"):
            key = prefix + path.relative_to(source_dir).as_posix()
            with_retry(lambda: s3.upload_file(str(path), bucket, key), f"s3://{bucket}/{key}",
                       retry_on=(BotoCoreError, ClientError))
            uploaded += 1
    return uploaded


def latest_partition(s3: Any, bucket: str, prefix: str, partition: str) -> str:
    """Return the newest ``partition=value/`` prefix under ``prefix``."""
    values = {m.group(1) for k in list_keys(s3, bucket, prefix)
              if (m := re.search(rf"{partition}=([^/]+)/", k[len(prefix):]))}
    if not values:
        raise FileNotFoundError(f"No {partition}= partitions under s3://{bucket}/{prefix} - run the bronze stage first")
    return f"{prefix}{partition}={max(values)}/"


@dataclass
class EntityResult:
    """Counts for one entity; ``reconciled`` proves no row was silently lost."""

    entity: str
    rows_read: int
    duplicates_removed: int
    rows_rejected: int
    rows_written: int

    @property
    def reconciled(self) -> bool:
        return self.rows_read == self.duplicates_removed + self.rows_rejected + self.rows_written


def apply_contract(spark: Any, spec: EntitySpec, records: Any, parents: dict[str, Any],
                   run: RunContext, extra: Iterable[tuple[str, Any]]) -> tuple[Any, list[dict[str, Any]], int]:
    """Validate an RDD of ``(ok, dict)`` pairs, drop PK duplicates and enforce FKs.

    Returns ``(valid DataFrame, rejected records, duplicates removed)``.
    """
    from pyspark.sql import functions as F

    extra = list(extra)
    schema = spark_schema(spec.model, extra)
    names = schema.fieldNames()
    records = records.cache()
    rejected = records.filter(lambda pair: not pair[0]).map(lambda pair: pair[1]).collect()
    valid_rows = records.filter(lambda pair: pair[0]).map(lambda pair: tuple(pair[1].get(n) for n in names))
    valid = spark.createDataFrame(valid_rows, schema)

    before = valid.count()
    valid = valid.dropDuplicates(list(spec.primary_key))
    duplicates = before - valid.count()

    for fk in spec.foreign_keys:
        keys = parents[fk.parent].select(F.col(fk.parent_key).alias(fk.column)).distinct()
        orphans = valid.join(keys, on=fk.column, how="left_anti")
        for row in orphans.collect():
            rejected.append({"reason": f"unknown_{fk.column}",
                             "errors": [f"{fk.column}={row[fk.column]} not found in {fk.parent}"],
                             "record": row.asDict(), "_source_file": row.asDict().get("_source_file")})
        valid = valid.join(keys, on=fk.column, how="left_semi")

    valid = (valid.withColumn("_silver_run_id", F.lit(run.run_id))
             .withColumn("_silver_processed_at", F.lit(run.started_at.isoformat()))
             .select(*names, "_silver_run_id", "_silver_processed_at"))
    return valid.cache(), rejected, duplicates


def write_quarantine(s3: Any, settings: Settings, run: RunContext, entity: str, rejected: list[dict[str, Any]]) -> str | None:
    """Write rejected records (with reasons) to the quarantine area; overwrite per day."""
    key = f"silver/_quarantine/{entity}/ingest_date={run.ingest_date}/rejected.jsonl"
    if not rejected:
        s3.delete_object(Bucket=settings.bucket, Key=key)
        return None
    body = "\n".join(json.dumps({"entity": entity, "run_id": run.run_id, **r}, default=str) for r in rejected)
    s3.put_object(Bucket=settings.bucket, Key=key, Body=body.encode(), ContentType="application/x-ndjson")
    return f"s3://{settings.bucket}/{key}"


def write_silver(s3: Any, settings: Settings, spec: EntitySpec, df: Any, workdir: Path) -> int:
    """Write a Silver entity as Parquet and replace its S3 prefix (idempotent)."""
    out = workdir / "silver" / spec.name
    writer = df.coalesce(1).write.mode("overwrite")
    if spec.partition_by:
        writer = writer.partitionBy(*spec.partition_by)
    writer.parquet(str(out))
    return replace_prefix(s3, settings.bucket, f"silver/{spec.name}/", out)


# ---------------------------------------------------------------------------
# Silver layer: stage
# ---------------------------------------------------------------------------
def silver_tables(spark: Any, s3: Any, settings: Settings, run: RunContext, workdir: Path,
                  parents: dict[str, Any]) -> list[EntityResult]:
    """Validate and conform the latest Bronze snapshot of every PostgreSQL table."""
    from pyspark.sql import types as T

    results = []
    for spec in TABLE_SPECS:
        prefix = latest_partition(s3, settings.bucket, f"{settings.bronze_prefix}postgres/{spec.name}/", "ingest_date")
        local = workdir / "bronze" / spec.name
        download_prefix(s3, settings.bucket, prefix, local)
        bronze = spark.read.parquet(str(local))
        rows_read = bronze.count()
        model = spec.model
        records = bronze.rdd.map(lambda row, m=model: _validate_table_row(row.asDict(), m))
        valid, rejected, duplicates = apply_contract(
            spark, spec, records, parents, run, extra=[("_bronze_run_id", T.StringType())])
        parents[spec.name] = valid
        result = EntityResult(spec.name, rows_read, duplicates, len(rejected), valid.count())
        write_silver(s3, settings, spec, valid, workdir)
        write_quarantine(s3, settings, run, spec.name, rejected)
        log_entity(result, source=f"s3://{settings.bucket}/{prefix}")
        results.append(result)
    return results


def _validate_table_row(raw: dict[str, Any], model: type[SilverModel]) -> tuple[bool, dict[str, Any]]:
    """Validate one Bronze table row and carry its Bronze run id for lineage."""
    ok, out = validate_record(raw, model)
    if ok:
        out["_bronze_run_id"] = raw.get("_ingest_run_id")
    return ok, out


def silver_events(spark: Any, s3: Any, settings: Settings, run: RunContext, workdir: Path,
                  parents: dict[str, Any]) -> EntityResult:
    """Parse, validate, de-duplicate and conform the raw learning-activity events."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    prefix = f"{settings.bronze_prefix}learning_activity/"
    local = workdir / "bronze" / "learning_activity"
    files = download_prefix(s3, settings.bucket, prefix, local)
    if files == 0:
        raise FileNotFoundError(f"No Bronze event files under s3://{settings.bucket}/{prefix}")

    lines = (spark.read.text(str(local / "year=*" / "month=*" / "day=*" / "*.jsonl"))
             .select(F.col("value"), F.col("_metadata.file_path").alias("file"))
             .filter(F.trim("value") != ""))
    rows_read = lines.count()
    # Exact duplicate lines are the same event delivered twice: keep one copy.
    unique_lines = lines.dropDuplicates(["value"])
    exact_duplicates = rows_read - unique_lines.count()

    root = str(local)
    records = unique_lines.rdd.map(
        lambda row: validate_event_line(row["value"], prefix + row["file"].split(root, 1)[-1].lstrip("/")))
    valid, rejected, pk_duplicates = apply_contract(
        spark, EVENT_SPEC, records, parents, run, extra=[("_source_file", T.StringType())])
    valid = (valid.withColumn("event_year", F.year("event_date"))
             .withColumn("event_month", F.month("event_date")))
    result = EntityResult(EVENT_SPEC.name, rows_read, exact_duplicates + pk_duplicates, len(rejected), valid.count())
    write_silver(s3, settings, EVENT_SPEC, valid, workdir)
    write_quarantine(s3, settings, run, EVENT_SPEC.name, rejected)
    reasons: dict[str, int] = {}
    for item in rejected:
        reasons[item["reason"]] = reasons.get(item["reason"], 0) + 1
    log_entity(result, source=f"s3://{settings.bucket}/{prefix}", files=files, rejected_by_reason=reasons)
    return result


def log_entity(result: EntityResult, **fields: Any) -> None:
    """Log one entity's reconciliation; WARN when rows were rejected."""
    level = logging.WARNING if result.rows_rejected else logging.INFO
    log(level, "silver entity written", entity=result.entity, rows_read=result.rows_read,
        duplicates_removed=result.duplicates_removed, rows_rejected=result.rows_rejected,
        rows_written=result.rows_written, reconciled=result.reconciled, **fields)
    if not result.reconciled:
        raise RuntimeError(f"Reconciliation failed for {result.entity}: {result}")


def transform_to_silver(settings: Settings, run: RunContext) -> dict[str, Any]:
    """Build the Silver layer from Bronze: validate, clean, de-duplicate, conform."""
    s3 = s3_client(settings)
    spark = create_spark_session()
    results: list[EntityResult] = []
    status = "FAILED"
    try:
        with tempfile.TemporaryDirectory(prefix="acme-silver-") as tmp:
            workdir = Path(tmp)
            parents: dict[str, Any] = {}
            results.extend(silver_tables(spark, s3, settings, run, workdir, parents))
            results.append(silver_events(spark, s3, settings, run, workdir, parents))
        status = "SUCCEEDED"
    finally:
        records = [{"source": f"bronze/{r.entity}", "target": f"silver/{r.entity}", **r.__dict__,
                    "reconciled": r.reconciled} for r in results]
        key = f"silver/_lineage/ingest_date={run.ingest_date}/silver-{run.run_id}.json"
        s3.put_object(Bucket=settings.bucket, Key=key, ContentType="application/json",
                      Body=json.dumps({"run_id": run.run_id, "stage": "silver", "status": status,
                                       "records": records}, indent=2, default=str).encode())
        spark.stop()
    return {
        "entities": len(results),
        "rows_written": sum(r.rows_written for r in results),
        "rows_rejected": sum(r.rows_rejected for r in results),
        "duplicates_removed": sum(r.duplicates_removed for r in results),
    }


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
