#!/usr/bin/env bash
# Shared setup for run_local.sh and run_tests.sh (source this file, do not execute it).
# - loads data/acme-performance/.env.local
# - uses LocalStack test credentials when IS_LOCAL=true
# - fixes JAVA_HOME / SPARK_HOME for Spark
# - creates or updates the Python virtual environment

JOB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ENV_FILE="$JOB_DIR/.env.local"
VENV="$JOB_DIR/.venv"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found. Run: cp $JOB_DIR/.env.sample $ENV_FILE" >&2
    exit 1
fi

# Export every variable defined in .env.local.
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

# Spark needs a valid JAVA_HOME. If it is unset or points nowhere (the VDI's ~/.bashrc
# can contain java-$JAVA_VERSION-... with an empty variable), derive it from `java`.
if [ ! -x "${JAVA_HOME:-/nonexistent}/bin/java" ]; then
    if command -v java >/dev/null 2>&1; then
        JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
        export JAVA_HOME
        echo "INFO: JAVA_HOME was invalid; using $JAVA_HOME"
    else
        echo "ERROR: Java is required for Spark but was not found on PATH." >&2
        exit 1
    fi
fi

# install_requirements FILE...: (re)install only when the files changed since last time.
install_requirements() {
    if [ ! -d "$VENV" ]; then
        echo "INFO: Creating virtual environment (first run only)..."
        python3 -m venv "$VENV"
        "$VENV/bin/pip" install --quiet --upgrade pip
    fi
    local stamp="$VENV/.requirements.sha256" hash
    hash="$(cat "$@" | sha256sum | cut -d' ' -f1)"
    if ! grep -qx "$hash" "$stamp" 2>/dev/null; then
        echo "INFO: Installing requirements: $*"
        for req in "$@"; do "$VENV/bin/pip" install --quiet -r "$req"; done
        echo "$hash" >> "$stamp"
    fi
}
