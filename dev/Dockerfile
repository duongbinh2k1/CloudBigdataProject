# One machine of the local cluster, installed like the EC2 nodes
FROM ubuntu:22.04

COPY cluster/ /opt/cluster/
RUN bash /opt/cluster/install.sh

# containers only: same SSH key everywhere, list of workers, daemons run as root
RUN ssh-keygen -q -t ed25519 -N "" -f /root/.ssh/id_ed25519 \
 && cp /root/.ssh/id_ed25519.pub /root/.ssh/authorized_keys \
 && printf 'Host *\n  StrictHostKeyChecking no\n  LogLevel ERROR\n' > /root/.ssh/config
COPY dev/workers /opt/hadoop/etc/hadoop/workers
COPY dev/workers /opt/spark/conf/workers
RUN printf 'export HDFS_NAMENODE_USER=root\nexport HDFS_DATANODE_USER=root\nexport HDFS_SECONDARYNAMENODE_USER=root\n' \
      >> /opt/hadoop/etc/hadoop/hadoop-env.sh

COPY dev/entrypoint.sh /entrypoint.sh
WORKDIR /opt/work
ENTRYPOINT ["bash", "-l", "/entrypoint.sh"]
