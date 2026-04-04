#!/bin/bash
set -euo pipefail


INPUT_PATH=${1:-/input/data}
STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar
SPLIT_SIZE=1073741824

if ! hdfs dfs -test -e "$INPUT_PATH"; then
    echo "Input path not found in HDFS: $INPUT_PATH"
    exit 1
fi

hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /indexer

echo "Pipeline 1: building /indexer/index"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.split.maxsize="$SPLIT_SIZE" \
    -D mapreduce.input.fileinputformat.split.minsize="$SPLIT_SIZE" \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output /indexer/index

echo "Pipeline 2: building /indexer/vocabulary"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.split.maxsize="$SPLIT_SIZE" \
    -D mapreduce.input.fileinputformat.split.minsize="$SPLIT_SIZE" \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input /indexer/index \
    -output /indexer/vocabulary

echo "Pipeline 3: building /indexer/documents"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.split.maxsize="$SPLIT_SIZE" \
    -D mapreduce.input.fileinputformat.split.minsize="$SPLIT_SIZE" \
    -files mapreduce/mapper3.py,mapreduce/reducer3.py \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input "$INPUT_PATH" \
    -output /indexer/documents

echo "Pipeline 4: building /indexer/stats"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.split.maxsize="$SPLIT_SIZE" \
    -D mapreduce.input.fileinputformat.split.minsize="$SPLIT_SIZE" \
    -files mapreduce/mapper4.py,mapreduce/reducer4.py \
    -mapper "python3 mapper4.py" \
    -reducer "python3 reducer4.py" \
    -input /indexer/documents \
    -output /indexer/stats

echo "Index created in HDFS"
hdfs dfs -ls /indexer
