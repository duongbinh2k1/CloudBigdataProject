#!/bin/bash
# Whole study on the master: bash bench/run_all.sh dev|aws (steps already done are skipped)
set -e
env=$1
config=bench/experiments-$env.yaml
results=results/$env
cd /opt/work

yaml() {
  python3 -c "import yaml; print(' '.join(str(v) for v in yaml.safe_load(open('$config'))['$1']))"
}
sizes=$(yaml sizes)
codecs=$(yaml storage_codecs | tr ' ' ',')

# 1. build the jar
bash java/build.sh

# 2. datasets: every size, one copy per storage codec
mkdir -p $results/datasets
for sf in $sizes; do
  if [ -f $results/datasets/sf$sf.txt ]; then
    echo "dataset SF $sf already generated"
    continue
  fi
  spark-submit --class compression.datagen.Generator java/build/cloud-bigdata-project.jar $sf hdfs://master:9000/data $codecs
  hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > $results/datasets/sf$sf.txt
done

# 3. codec micro-benchmark on the first lineitem file of the smallest size
if [ ! -f $results/codec_bench/raw.csv ]; then
  first=$(echo $sizes | cut -d' ' -f1)
  bash bench/codec_bench.sh hdfs://master:9000/data/sf$first/none/lineitem/part-00000 $results/codec_bench
fi

# 4. experiments
python3 bench/run_experiments.py $config $results
