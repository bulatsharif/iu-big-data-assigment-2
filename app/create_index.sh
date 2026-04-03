#!/bin/bash

hdfs dfs -rm -r /index
hdfs dfs -mkdir -p /index
hdfs dfs -put data /index/data

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "mapreduce/mapper1.py" \
    -reducer "mapreduce/reducer1.py" \
    -input /index/data \
    -output /index/output
