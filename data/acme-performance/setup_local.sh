#!/usr/bin/env bash
# Script: Prepare the local (VDI) environment for run_local.sh
# Usage:  ./data/acme-performance/setup_local.sh
#
# Safe to run any time (idempotent):
#   1. checks PostgreSQL on localhost:5432
#   2. starts LocalStack if it is not running, and waits until S3 is ready
#   3. creates the local data lake bucket if missing
#   4. uploads the sample learning-event files to landing/ (only new/changed files)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
ENV_FILE="$ROOT/data/acme-performance/.env.local"
DATASET="${ACME_DATASET_DIR:-$ROOT/sampleData/data-engineering-performance-talent-management}"
ENDPOINT="http://localhost:4566"

BUCKET="coding-workshop-datalake-1bd9f4ac"
if [ -f "$ENV_FILE" ]; then
    BUCKET="$(grep -E '^DATA_LAKE_BUCKET=' "$ENV_FILE" | cut -d= -f2- || true)"
    BUCKET="${BUCKET:-coding-workshop-datalake-1bd9f4ac}"
fi

# LocalStack accepts any credentials; never use real AWS keys for local runs
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
unset AWS_SESSION_TOKEN AWS_PROFILE

echo "== 1/4 PostgreSQL =="
if command -v pg_isready >/dev/null && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
    echo "OK: PostgreSQL is accepting connections"
else
    echo "WARN: PostgreSQL is not answering on localhost:5432 - start it with ./bin/start-dev.sh"
fi

echo "== 2/4 LocalStack =="
s3_ready() { curl -s "$ENDPOINT/_localstack/health" 2>/dev/null | grep -Eq '"s3": *"(running|available)"'; }
if s3_ready; then
    echo "OK: LocalStack is already running"
else
    if command -v localstack >/dev/null; then
        echo "INFO: starting LocalStack (about 30-60 seconds) ..."
        localstack start -d >/dev/null
    else
        echo "ERROR: 'localstack' command not found. Start the local environment with ./bin/start-dev.sh, then rerun this script." >&2
        exit 1
    fi
    for _ in $(seq 1 60); do
        s3_ready && break
        sleep 2
    done
    s3_ready || { echo "ERROR: LocalStack did not become ready. Check: localstack logs" >&2; exit 1; }
    echo "OK: LocalStack is running"
fi

echo "== 3/4 Data lake bucket =="
if aws --endpoint-url "$ENDPOINT" s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    echo "OK: s3://$BUCKET exists"
else
    aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" >/dev/null
    echo "OK: created s3://$BUCKET"
fi

echo "== 4/4 Landing files =="
aws --endpoint-url "$ENDPOINT" s3 sync "$DATASET/learning_activity" \
    "s3://$BUCKET/landing/learning_activity/" --only-show-errors
echo "OK: landing files in LocalStack: $(aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/landing/" --recursive | wc -l)"
echo ""
echo "Ready. Now run: ./data/acme-performance/run_local.sh --stage all"
