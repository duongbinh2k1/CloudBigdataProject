#!/bin/bash
# start hdfs, spark and the history server (on the master)
set -e

# hdfs (format the namenode only the first time)
[ -d /data/hdfs/namenode/current ] || hdfs namenode -format -nonInteractive
stop-dfs.sh || true
start-dfs.sh
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /spark-logs /data

# spark
stop-history-server.sh || true
stop-workers.sh || true
stop-master.sh || true
start-master.sh
start-workers.sh
start-history-server.sh
