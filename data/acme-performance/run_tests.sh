#!/usr/bin/env bash
# Script: Run the test suite with coverage and save the evidence
# Usage:  ./data/acme-performance/run_tests.sh [extra pytest args, e.g. -m "not integration"]
#
# Unit + Spark tests need nothing external. The integration test also needs the
# PostgreSQL source from .env.local and the sample dataset (sampleData/ or ACME_DATASET_DIR);
# it uses an in-memory S3, so it never touches LocalStack or AWS buckets.
set -euo pipefail

# shellcheck source=env_setup.sh
source "$(dirname "$0")/env_setup.sh"
install_requirements "$JOB_DIR/requirements.txt" "$JOB_DIR/tests/requirements-test.txt"

mkdir -p "$JOB_DIR/logs"
REPORT="$JOB_DIR/logs/test-report-$(date -u +%Y%m%dT%H%M%SZ).txt"
cd "$JOB_DIR"
set +e
"$VENV/bin/python" -m pytest --cov=job --cov-report=term-missing:skip-covered "$@" 2>&1 \
    | grep -vE "WARN (NativeCodeLoader|Utils)|log4j|setLogLevel" | tee "$REPORT"
STATUS="${PIPESTATUS[0]}"
set -e
echo "INFO: Test report saved to $REPORT"
exit "$STATUS"
