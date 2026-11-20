# Experimental Evaluation of Data Compression in Spark

InfraCloud project (USTH / INP-ENSEEIHT). We measure the effect of compression
(Gzip, Snappy, LZ4, Zstd, none) in Spark at two levels:

- **storage**: text files on HDFS written with a Hadoop codec,
- **communication**: shuffle data compressed by Spark,

with three applications (I/O-bound, CPU-bound, balanced) and four dataset sizes.

Java 11, Hadoop 3.4.1 (HDFS), Spark 3.5.5 standalone, Spark jobs in Java (RDD), Python 3 for the
experiment script and the plots, sysstat (`sar`).

| Folder | Content |
|---|---|
| `java/` | data generator and the 3 Spark applications, `build.sh` |
| `bench/` | `run_experiments.py` + `metrics.py`, the experiment files (YAML), `CodecBench.java` |
| `analysis/` | `report.py`: figures and tables |
| `dev/` | local cluster with containers (master + 2 workers) |
| `infra/` | Terraform (EC2) and Ansible (`cluster.yml`) |
| `results/` | raw results |
| `docs/` | plan, decisions, notes |

## Local cluster

```bash
make up                 # build the image and start the containers
make gen SF=0.1         # dataset
make experiments        # bench/experiments-dev.yaml -> results/dev/dev/runs.csv
python3 -m venv .venv && .venv/bin/pip install -r analysis/requirements.txt
make analysis           # -> analysis/figures/dev/dev/
```

HDFS: http://localhost:9870, Spark: http://localhost:8080, History Server: http://localhost:18080

## AWS cluster

```bash
ssh-keygen -t ed25519 -f ~/.ssh/compression-study
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # put your IP
terraform init && terraform apply                 # 4 x m5.xlarge
cd ../ansible && ansible-playbook cluster.yml     # install + start HDFS and Spark
ssh -i ~/.ssh/compression-study ubuntu@$(terraform -chdir=../terraform output -raw master_public_ip)
```

On the master:

```bash
cd /opt/work && mkdir -p results/aws/datasets
for sf in 1 5 10 20; do
  spark-submit --class compression.datagen.Generator java/build/compression-study.jar $sf hdfs://master:9000/data none,gzip,snappy,lz4,zstd
  hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > results/aws/datasets/sf$sf.txt
done
bash bench/codec_bench.sh results/aws/codec_bench
tmux new -s exp "python3 bench/run_experiments.py bench/experiments-aws.yaml results/aws"   # Ctrl-b d to leave it running
```

Web UIs from the laptop: `ssh -i ~/.ssh/compression-study -L 8080:master:8080 -L 18080:master:18080 -L 9870:master:9870 ubuntu@<master ip>`

At the end, copy the results and delete the cluster:

```bash
rsync -az -e "ssh -i ~/.ssh/compression-study" ubuntu@<master ip>:/opt/work/results/aws/ results/aws/
cd infra/terraform && terraform destroy
.venv/bin/python analysis/report.py results/aws/aws
```
