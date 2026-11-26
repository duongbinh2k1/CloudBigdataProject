#!/bin/bash
# Installs Java, Hadoop and Spark on Ubuntu 22.04 (local containers and EC2)
#   bash install.sh [user running the daemons, default root]
set -e
user=${1:-root}
cd "$(dirname "$0")"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y openjdk-11-jdk-headless python3-yaml sysstat libzstd1 openssh-server sudo curl tmux

# Hadoop and Spark: download and unpack (only the first time)
[ -d /opt/hadoop-3.4.1 ] || curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt
[ -d /opt/spark-3.5.5-bin-hadoop3 ] || curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt
ln -sfn /opt/hadoop-3.4.1 /opt/hadoop
ln -sfn /opt/spark-3.5.5-bin-hadoop3 /opt/spark

# environment variables of every login shell
cat > /etc/profile.d/bigdata.sh <<'END'
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
END

# configuration
cp hadoop/* /opt/hadoop/etc/hadoop/
cp spark/* /opt/spark/conf/

# directories written by the daemons: HDFS data, logs, project code
mkdir -p /data/hdfs /opt/work
chown -R "$user" /data/hdfs /opt/work /opt/hadoop-3.4.1 /opt/spark-3.5.5-bin-hadoop3
