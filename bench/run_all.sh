#!/bin/bash
# run everything on the master (make run)
set -e
cd /opt/work
mkdir -p results/datasets

# build
bash java/build.sh

# generate the datasets (same sizes and codecs as in run_experiments.py)
for sf in 1 5 10 20; do
  if [ ! -f results/datasets/sf$sf.txt ]; then
    spark-submit --class compression.DataGen java/build/app.jar $sf hdfs://master:9000/data none,lz4,zstd
    hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > results/datasets/sf$sf.txt
  fi
done

# codec benchmark
if [ ! -f results/codec_bench.csv ]; then
  hdfs dfs -get -f hdfs://master:9000/data/sf1/none/lineitem/part-00000 /tmp/lineitem.txt
  java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java /tmp/lineitem.txt results/codec_bench.csv
fi

# experiments
python3 bench/run_experiments.py
echo "all done"
