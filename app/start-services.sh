#!/bin/bash
# This will run only by the master node

printf 'cluster-slave-1\n' > "$HADOOP_HOME/etc/hadoop/workers"

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting YARN daemons
yarn --daemon start resourcemanager

# Start mapreduce history server
mapred --daemon start historyserver

while true
do
    if ssh -o StrictHostKeyChecking=no cluster-slave-1 "echo ready" >/dev/null 2>&1; then
        break
    fi

    sleep 2
done

ssh -o StrictHostKeyChecking=no cluster-slave-1 \
    "jps -lm | grep -q org.apache.hadoop.hdfs.server.datanode.DataNode || $HADOOP_HOME/bin/hdfs --daemon start datanode"

ssh -o StrictHostKeyChecking=no cluster-slave-1 \
    "jps -lm | grep -q org.apache.hadoop.yarn.server.nodemanager.NodeManager || $HADOOP_HOME/bin/yarn --daemon start nodemanager"


# track process IDs of services
jps -lm

# subtool to perform administrator functions on HDFS
# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -safemode leave

while true
do
    REPORT="$(hdfs dfsadmin -report 2>/dev/null || true)"

    if printf '%s\n' "$REPORT" | grep -Eq 'Live datanodes \([1-9]'; then
        break
    fi

    sleep 2
done

while true
do
    NODES="$(yarn node -list 2>/dev/null || true)"

    if printf '%s\n' "$NODES" | grep -Eq 'Total Nodes:[[:space:]]*[1-9]'; then
        break
    fi

    sleep 2
done

# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -report

# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root
