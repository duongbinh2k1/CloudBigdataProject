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
| `docs/` | plan, decisions, notes, AWS checklist |

## How to run

The same script runs everything on the master node: `bench/run_all.sh dev` on the local cluster,
`bench/run_all.sh aws` on EC2. It builds the jar, generates the datasets, runs the codec
micro-benchmark and the experiments of `bench/experiments-<env>.yaml` (steps already done are skipped).
The figures are made on the laptop with `analysis/report.py`.

## Local cluster

```bash
make up                 # build the image and start master + 2 workers
make run                # bench/run_all.sh dev  -> results/dev/
python3 -m venv .venv && .venv/bin/pip install -r analysis/requirements.txt    # once
make analysis           # -> analysis/figures/dev/dev/
```

HDFS: http://localhost:9870, Spark: http://localhost:8080, History Server: http://localhost:18080

## AWS cluster

Checklist, checks and cost: [docs/AWS.md](docs/AWS.md).

```bash
ssh-keygen -t ed25519 -f ~/.ssh/cloud-bigdata-project
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # put your IP
terraform init && terraform apply                 # 4 x m5.xlarge
cd ../ansible && ansible-playbook cluster.yml     # install + start HDFS and Spark
ssh -i ~/.ssh/cloud-bigdata-project ubuntu@$(terraform -chdir=../terraform output -raw master_public_ip)
```

On the master:

```bash
cd /opt/work && tmux new -s exp "bash bench/run_all.sh aws"    # Ctrl-b d to leave it running, tmux attach -t exp to come back
```

Web UIs from the laptop: `ssh -i ~/.ssh/cloud-bigdata-project -L 8080:master:8080 -L 18080:master:18080 -L 9870:master:9870 ubuntu@<master ip>`

At the end, copy the results and delete the cluster:

```bash
rsync -az -e "ssh -i ~/.ssh/cloud-bigdata-project" ubuntu@<master ip>:/opt/work/results/aws/ results/aws/
cd infra/terraform && terraform destroy
cd ../.. && make analysis RESULTS=results/aws/aws
```
