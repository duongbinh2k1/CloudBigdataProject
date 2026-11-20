#!/bin/bash
# usage: bench/codec_bench.sh <lineitem file on HDFS> <output dir>
set -e
mkdir -p "$2"
hdfs dfs -get -f "$1" /tmp/lineitem.csv
java -Xmx2g -cp "$SPARK_HOME/jars/*" bench/CodecBench.java "$2/raw.csv" /tmp/lineitem.csv
