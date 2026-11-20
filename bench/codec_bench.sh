#!/bin/bash
# usage: bench/codec_bench.sh <output dir>   (needs the SF 1 dataset without compression)
mkdir -p "$1"
hdfs dfs -get -f hdfs://master:9000/data/sf1/none/lineitem/part-00000 /tmp/lineitem.csv
java -Xmx2g -cp "$SPARK_HOME/jars/*" bench/CodecBench.java "$1/raw.csv" /tmp/lineitem.csv
