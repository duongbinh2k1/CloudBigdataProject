export JAVA_HOME={{ java_home }}
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES={{ worker_cores }}
export SPARK_WORKER_MEMORY={{ worker_memory_gb }}g
export SPARK_LOCAL_DIRS=/data/spark-local  # shuffle files and spills on the local disk
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://master:9000/spark-logs -Dspark.history.fs.update.interval=2s"
