#!/bin/bash
# master: NameNode + Spark Master + History Server
# worker: DataNode + Spark Worker
mkdir -p /data/hdfs /data/spark-local

if [ "$ROLE" = "master" ]; then
  if [ ! -d /data/hdfs/namenode/current ]; then
    hdfs namenode -format -nonInteractive
  fi
  hdfs --daemon start namenode
  hdfs dfsadmin -safemode wait
  hdfs dfs -mkdir -p /spark-logs /data /results
  start-master.sh
  start-history-server.sh
else
  sleep 15   # let the master start first
  hdfs --daemon start datanode
  start-worker.sh spark://master:7077
fi

# keep the container running
tail -f /dev/null
