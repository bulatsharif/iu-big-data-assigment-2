#!/bin/bash
set -euo pipefail

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

DOC_COUNT=$(find data -maxdepth 1 -type f -name "*.txt" | wc -l | tr -d ' ')

if [ "$DOC_COUNT" -eq 0 ]; then
    echo "No text documents found in ./data"
    exit 1
fi

echo "Uploading local documents to HDFS /data"
hdfs dfs -rm -r -f /data /input/data >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

echo "Building HDFS /input/data"
spark-submit prepare_data.py

echo "Verifying HDFS outputs"
hdfs dfs -ls /data
hdfs dfs -ls /input/data
