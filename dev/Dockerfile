# One image for all nodes of the local cluster (master or worker, see entrypoint.sh)
FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update && apt-get install -y python3 python3-yaml procps sysstat curl \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt \
 && mv /opt/hadoop-3.4.1 /opt/hadoop \
 && curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt \
 && mv /opt/spark-3.5.5-bin-hadoop3 /opt/spark

ENV HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

COPY conf/hadoop/ /opt/hadoop/etc/hadoop/
COPY conf/spark/ /opt/spark/conf/
COPY entrypoint.sh /entrypoint.sh
WORKDIR /opt/work
ENTRYPOINT ["bash", "/entrypoint.sh"]
