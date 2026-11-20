export JAVA_HOME={{ java_home }}
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=12g             # of 16 GB: the rest for the OS, the DataNode and the page cache
export SPARK_LOCAL_DIRS=/data/spark-local  # shuffle files and spills on the local disk
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://master:9000/spark-logs -Dspark.history.fs.update.interval=2s"
