#!/bin/bash
# Each container starts an SSH server, like a machine. The master then starts the whole
# cluster over SSH, with the same commands as on EC2 and in the course.
service ssh start

if [ "$ROLE" = "master" ]; then
  for w in $(cat /opt/hadoop/etc/hadoop/workers); do
    until ssh $w true; do sleep 2; done        # wait for the workers' SSH servers
  done
  [ -d /data/hdfs/namenode/current ] || hdfs namenode -format -nonInteractive
  start-dfs.sh
  hdfs dfsadmin -safemode wait
  hdfs dfs -mkdir -p /spark-logs /data /results
  start-master.sh
  start-workers.sh
  start-history-server.sh
fi

tail -f /dev/null   # keep the container running
