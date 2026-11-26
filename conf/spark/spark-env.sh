export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java))))
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
