export JAVA_HOME={{ java_home }}
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
# each worker gives Spark all its cores and its RAM minus 3 GB (Linux, DataNode)
export SPARK_WORKER_CORES={{ cores }}
export SPARK_WORKER_MEMORY={{ ram_gb | int - 3 }}g
export SPARK_DAEMON_MEMORY=512m
export SPARK_LOCAL_DIRS=/data/spark-local  # shuffle files and spills on the local disk
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://master:9000/spark-logs -Dspark.history.fs.update.interval=2s"
