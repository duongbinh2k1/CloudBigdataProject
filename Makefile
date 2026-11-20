# Local cluster (Podman). With Docker: make up DC="docker compose -f dev/compose.yml"
DC = podman compose -f dev/compose.yml
RUN = $(DC) exec -w /opt/work master
SF ?= 0.1

up:
	$(DC) up -d --build

down:
	$(DC) down

shell:
	$(DC) exec master bash

jar:
	$(RUN) bash java/build.sh

# make gen SF=1   (zstd only works on the EC2 cluster)
gen: jar
	$(RUN) spark-submit --class compression.datagen.Generator java/build/compression-study.jar \
		$(SF) hdfs://master:9000/data none,gzip,snappy,lz4
	$(RUN) bash -c "hdfs dfs -du -s 'hdfs://master:9000/data/sf$(SF)/*/*' > results/dev/datasets/sf$(SF).txt"

experiments: jar
	$(RUN) python3 bench/run_experiments.py bench/experiments-dev.yaml results/dev

codec-bench:
	$(RUN) bash bench/codec_bench.sh results/dev/codec_bench
