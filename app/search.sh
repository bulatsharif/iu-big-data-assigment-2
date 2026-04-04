#!/bin/bash
set -euo pipefail

source .venv/bin/activate

QUERY="$*"

if [ -z "$QUERY" ]; then
    echo "Usage: bash search.sh <query>"
    exit 1
fi

export PYSPARK_DRIVER_PYTHON="$(which python)"
export PYSPARK_PYTHON=./.venv/bin/python

printf '%s\n' "$QUERY" | spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    query.py
