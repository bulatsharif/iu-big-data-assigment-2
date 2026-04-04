#!/bin/bash
set -euo pipefail

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

DOC_LIMIT=100

DOC_COUNT=$(find data -maxdepth 1 -type f -name "*.txt" | wc -l | tr -d ' ')

if [ "$DOC_COUNT" -eq 0 ]; then
    echo "No text documents found in ./data"
    exit 1
fi

echo "Uploading local documents to HDFS /data"
hdfs dfs -rm -r -f /data /input/data >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /data

mapfile -t DOC_FILES < <(
    find data -maxdepth 1 -type f -name "*.txt" \
    | LC_ALL=C grep '^[ -~]*$' \
    | sort \
    | sed -n "1,${DOC_LIMIT}p"
)

hdfs dfs -put -f "${DOC_FILES[@]}" /data/

echo "Building HDFS /input/data"
spark-submit prepare_data.py

echo "Verifying HDFS outputs"
hdfs dfs -ls /data
hdfs dfs -ls /input/data
