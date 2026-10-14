#!/usr/bin/env bash
# Script: Run the ACME performance data job locally (VDI + LocalStack S3 + local PostgreSQL)
# Usage:  ./data/acme-performance/run_local.sh [--stage bronze|silver|gold|all]
#
# Reads settings from data/acme-performance/.env.local (copy .env.sample first).
# Creates a Python virtual environment in data/acme-performance/.venv on first run.
set -euo pipefail

JOB_DIR="$(cd "$(dirname "$0")" && pwd -P)"
ENV_FILE="$JOB_DIR/.env.local"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found. Run: cp $JOB_DIR/.env.sample $ENV_FILE" >&2
    exit 1
fi

# Export every variable defined in .env.local for the job process.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# LocalStack accepts any credentials; never use the real workshop AWS keys locally.
if [ "${IS_LOCAL:-true}" = "true" ]; then
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
    unset AWS_SESSION_TOKEN AWS_PROFILE
fi

# Use the PySpark version pinned in requirements.txt, not a separately installed Spark.
unset SPARK_HOME

if [ ! -d "$JOB_DIR/.venv" ]; then
    echo "INFO: Creating virtual environment (first run only)..."
    python3 -m venv "$JOB_DIR/.venv"
    "$JOB_DIR/.venv/bin/pip" install --quiet --upgrade pip
fi
# Re-install only when requirements.txt changed since the last install.
REQ_STAMP="$JOB_DIR/.venv/.requirements.sha256"
REQ_HASH="$(sha256sum "$JOB_DIR/requirements.txt" | cut -d' ' -f1)"
if [ "$(cat "$REQ_STAMP" 2>/dev/null)" != "$REQ_HASH" ]; then
    echo "INFO: Installing requirements..."
    "$JOB_DIR/.venv/bin/pip" install --quiet -r "$JOB_DIR/requirements.txt"
    echo "$REQ_HASH" > "$REQ_STAMP"
fi

mkdir -p "$JOB_DIR/logs"
LOG_FILE="$JOB_DIR/logs/run-$(date -u +%Y%m%dT%H%M%SZ).log"
echo "INFO: Logging to $LOG_FILE"
"$JOB_DIR/.venv/bin/python" "$JOB_DIR/job.py" "$@" 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
