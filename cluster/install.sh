#!/bin/bash
# Installs Java 11, Hadoop 3.4.1 and Spark 3.5.5 on one Ubuntu 22.04 machine (run by Ansible, as root)
set -e
cd "$(dirname "$0")"

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk-headless python3-yaml sysstat libzstd1 tmux

# download and unpack (only the first time)
[ -d /opt/hadoop-3.4.1 ] || curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt
[ -d /opt/spark-3.5.5-bin-hadoop3 ] || curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt
ln -sfn /opt/hadoop-3.4.1 /opt/hadoop
ln -sfn /opt/spark-3.5.5-bin-hadoop3 /opt/spark

# environment of every login shell
cat > /etc/profile.d/bigdata.sh <<'END'
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
END

# our configuration files
cp hadoop/* /opt/hadoop/etc/hadoop/
cp spark/* /opt/spark/conf/

# HDFS data and project code, owned by the user that runs everything
mkdir -p /data/hdfs /opt/work
chown -R ubuntu /data/hdfs /opt/work /opt/hadoop-3.4.1 /opt/spark-3.5.5-bin-hadoop3
