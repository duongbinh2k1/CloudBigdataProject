#!/bin/bash
# Every container runs an SSH server; the master starts the cluster
service ssh start

if [ "$ROLE" = "master" ]; then
  for w in $(cat /opt/hadoop/etc/hadoop/workers); do
    until ssh $w true; do sleep 2; done        # wait for the workers' SSH servers
  done
  bash -l /opt/cluster/start-cluster.sh
fi

tail -f /dev/null   # keep the container running
