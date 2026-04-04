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

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.jars=local:/usr/local/spark/jars/* \
    --archives /app/.venv.tar.gz#.venv \
    query.py "$@"
