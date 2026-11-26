# One "machine" of the local cluster: a plain Ubuntu, installed like the EC2 nodes
# (packages, Hadoop and Spark archives, the same conf/ files, passwordless SSH).
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y openjdk-11-jdk-headless python3-yaml sysstat \
      openssh-server sudo curl procps \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt \
 && curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt \
 && ln -s /opt/hadoop-3.4.1 /opt/hadoop && ln -s /opt/spark-3.5.5-bin-hadoop3 /opt/spark

ENV HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

# passwordless SSH between the nodes (every container has the same key)
RUN ssh-keygen -q -t ed25519 -N "" -f /root/.ssh/id_ed25519 \
 && cp /root/.ssh/id_ed25519.pub /root/.ssh/authorized_keys \
 && printf 'Host *\n  StrictHostKeyChecking no\n  LogLevel ERROR\n' > /root/.ssh/config

# the same configuration files as on EC2, and the list of workers
COPY conf/hadoop/ /opt/hadoop/etc/hadoop/
COPY conf/spark/ /opt/spark/conf/
COPY dev/workers /opt/hadoop/etc/hadoop/workers
COPY dev/workers /opt/spark/conf/workers
# here the daemons run as root (on EC2: as ubuntu)
RUN printf 'export HDFS_NAMENODE_USER=root\nexport HDFS_DATANODE_USER=root\nexport HDFS_SECONDARYNAMENODE_USER=root\n' \
      >> /opt/hadoop/etc/hadoop/hadoop-env.sh

COPY dev/entrypoint.sh /entrypoint.sh
WORKDIR /opt/work
ENTRYPOINT ["bash", "/entrypoint.sh"]
