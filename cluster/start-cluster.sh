#!/bin/bash
# Starts HDFS, Spark and the History Server, from the master (run by Ansible)
set -e

# HDFS: format the NameNode the first time only, then (re)start it
[ -d /data/hdfs/namenode/current ] || hdfs namenode -format -nonInteractive
stop-dfs.sh || true
start-dfs.sh
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /spark-logs /data /results

# Spark: master, workers, History Server
stop-history-server.sh || true
stop-workers.sh || true
stop-master.sh || true
start-master.sh
start-workers.sh
start-history-server.sh
