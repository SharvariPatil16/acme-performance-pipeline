#!/usr/bin/env bash
# Script: Run the ACME performance data job locally (VDI + LocalStack S3 + local PostgreSQL)
# Usage:  ./data/acme-performance/run_local.sh [--stage bronze|silver|gold|all]
#
# Reads settings from data/acme-performance/.env.local (copy .env.sample first).
# Creates a Python virtual environment in data/acme-performance/.venv on first run.
set -euo pipefail

# shellcheck source=env_setup.sh
source "$(dirname "$0")/env_setup.sh"
install_requirements "$JOB_DIR/requirements.txt"

mkdir -p "$JOB_DIR/logs"
LOG_FILE="$JOB_DIR/logs/run-$(date -u +%Y%m%dT%H%M%SZ).log"
echo "INFO: Logging to $LOG_FILE"
"$VENV/bin/python" "$JOB_DIR/job.py" "$@" 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
