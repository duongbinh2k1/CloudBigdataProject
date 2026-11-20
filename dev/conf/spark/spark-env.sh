export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1g
export SPARK_DAEMON_MEMORY=192m
export SPARK_LOCAL_DIRS=/data/spark-local
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://master:9000/spark-logs -Dspark.history.fs.update.interval=2s"
