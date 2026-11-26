#!/bin/bash
# The whole study, on the master (started by `make run`). Steps already done are skipped.
set -e
cd /opt/work
mkdir -p results/datasets
read_config() { python3 -c "import yaml; print(*yaml.safe_load(open('bench/experiments.yaml'))['$1'], sep='$2')"; }
sizes=$(read_config sizes ' ')
codecs=$(read_config storage_codecs ',')

# 1. compile the data generator and the 3 apps
bash java/build.sh

# 2. datasets on HDFS: every size, one copy per storage codec; their sizes go to results/datasets/
for sf in $sizes; do
  if [ ! -f results/datasets/sf$sf.txt ]; then
    spark-submit --class compression.DataGen java/build/app.jar $sf hdfs://master:9000/data $codecs
    hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > results/datasets/sf$sf.txt
  fi
done

# 3. codec micro-benchmark on one lineitem file (~75 MB) of the smallest size
if [ ! -f results/codec_bench.csv ]; then
  hdfs dfs -get -f hdfs://master:9000/data/sf${sizes%% *}/none/lineitem/part-00000 /tmp/lineitem.txt
  java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java /tmp/lineitem.txt results/codec_bench.csv
fi

# 4. the experiments
python3 bench/run_experiments.py
echo "all done"
