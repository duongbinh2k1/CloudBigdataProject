#!/bin/bash
# The whole study, on the master (started by `make run`). Steps already done are skipped.
set -e
cd /opt/work
mkdir -p results/datasets

# 1. compile the data generator and the 3 applications
bash java/build.sh

# 2. datasets on HDFS: every size, one copy per storage codec (same sizes and codecs as run_experiments.py)
for sf in 1 5 10 20; do
  if [ ! -f results/datasets/sf$sf.txt ]; then
    spark-submit --class compression.DataGen java/build/app.jar $sf hdfs://master:9000/data none,lz4,zstd
    hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > results/datasets/sf$sf.txt
  fi
done

# 3. codec micro-benchmark on one lineitem file (~75 MB)
if [ ! -f results/codec_bench.csv ]; then
  hdfs dfs -get -f hdfs://master:9000/data/sf1/none/lineitem/part-00000 /tmp/lineitem.txt
  java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java /tmp/lineitem.txt results/codec_bench.csv
fi

# 4. the 216 runs
python3 bench/run_experiments.py
echo "all done"
