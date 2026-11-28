#!/bin/bash
# install java, hadoop and spark (run by ansible)
set -e
cd "$(dirname "$0")"

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk-headless sysstat libzstd1 tmux

# download hadoop and spark
[ -d /opt/hadoop-3.4.1 ] || curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt
[ -d /opt/spark-3.5.5-bin-hadoop3 ] || curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt
ln -sfn /opt/hadoop-3.4.1 /opt/hadoop
ln -sfn /opt/spark-3.5.5-bin-hadoop3 /opt/spark

# environment variables
cat > /etc/profile.d/bigdata.sh <<'END'
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
END

# config files
cp hadoop/* /opt/hadoop/etc/hadoop/
cp spark/* /opt/spark/conf/

# folders
mkdir -p /data/hdfs /opt/work
chown -R ubuntu /data/hdfs /opt/work /opt/hadoop-3.4.1 /opt/spark-3.5.5-bin-hadoop3
