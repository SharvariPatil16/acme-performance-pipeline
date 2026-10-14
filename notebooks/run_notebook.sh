#!/usr/bin/env bash
# Script: Execute the ACME insights notebook against the Gold layer and save its outputs
# Usage:  ./notebooks/run_notebook.sh            (run the pipeline first)
#
# Reuses the pipeline's virtual environment and settings (data/acme-performance/.env.local),
# registers it as the Jupyter kernel "acme-performance", then runs every cell in place.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
VENV="$ROOT/data/acme-performance/.venv"
NOTEBOOK="$ROOT/notebooks/acme_performance_insights.ipynb"

if [ ! -x "$VENV/bin/python" ]; then
    echo "ERROR: $VENV not found. Run ./data/acme-performance/run_local.sh first." >&2
    exit 1
fi

"$VENV/bin/pip" install --quiet -r "$ROOT/notebooks/requirements.txt"
"$VENV/bin/python" -m ipykernel install --user --name acme-performance \
    --display-name "Python (acme-performance)" >/dev/null

# The notebook loads data/acme-performance/.env.local itself; run from the repo root.
cd "$ROOT"
"$VENV/bin/jupyter" nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=acme-performance \
    --ExecutePreprocessor.timeout=300 "$NOTEBOOK"
echo "INFO: Notebook executed: $NOTEBOOK (open it in Jupyter or VS Code to view the charts)"
