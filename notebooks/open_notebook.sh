#!/usr/bin/env bash
# Script: Open the ACME insights notebook in the browser (Jupyter Notebook)
# Usage:  ./notebooks/open_notebook.sh aws     # read the Gold layer from the AWS S3 data lake
#         ./notebooks/open_notebook.sh local   # read the Gold layer from LocalStack (VDI run)
# Then in the browser: Run > Run All Cells. Stop Jupyter with Ctrl+C in this terminal.
set -euo pipefail

MODE="${1:-aws}"
ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
VENV="$ROOT/data/acme-performance/.venv"

if [ ! -x "$VENV/bin/python" ]; then
    echo "ERROR: $VENV not found. Run ./data/acme-performance/run_local.sh first." >&2
    exit 1
fi
"$VENV/bin/pip" install --quiet -r "$ROOT/notebooks/requirements.txt"
"$VENV/bin/python" -m ipykernel install --user --name acme-performance \
    --display-name "Python (acme-performance)" >/dev/null

case "$MODE" in
  aws)
    # Fresh temporary participant credentials (official workshop script); never printed.
    "$ROOT/bin/setup-participant.sh" >/dev/null
    set -a
    # shellcheck disable=SC1091
    source "$ROOT/ENVIRONMENT.config"
    set +a
    export IS_LOCAL=false
    unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3   # real AWS, never LocalStack
    export AWS_REGION="${ACME_CLOUD_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${ACME_CLOUD_REGION:-ap-south-1}"
    export DATA_LAKE_BUCKET="${TF_VAR_aws_project:-coding-workshop}-datalake-${PARTICIPANT_ID}"
    echo "INFO: Notebook will read s3://$DATA_LAKE_BUCKET/gold/ in AWS ($AWS_REGION)"
    ;;
  local)
    echo "INFO: Notebook will read the Gold layer from LocalStack (settings in data/acme-performance/.env.local)"
    ;;
  *)
    echo "Usage: $0 aws|local" >&2
    exit 1
    ;;
esac

cd "$ROOT"
exec "$VENV/bin/jupyter" notebook notebooks/acme_performance_insights.ipynb
