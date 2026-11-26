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
| `cluster/` | the same for the local cluster and EC2: `install.sh` (Java, Hadoop, Spark), `start-cluster.sh` (start-dfs.sh, start-workers.sh…), configuration files |
| `dev/` | local cluster: Ubuntu containers installed and started with the scripts of `cluster/` (master + 2 workers) |
| `infra/` | Terraform (EC2) and Ansible (`cluster.yml`) |
| `results/` | raw results |
| `docs/` | how to run (`HUONG-DAN-CHAY.md`), study guide (`GUIDE.md`), line-by-line walkthrough (`walkthrough/`), defence questions (`QA.md`), plan, decisions, notes, AWS checklist |

## How to run

Step-by-step guide from an empty laptop (tools, `aws configure`, local run, AWS run, troubleshooting): [docs/HUONG-DAN-CHAY.md](docs/HUONG-DAN-CHAY.md).

All commands run on the laptop. `ENV=dev` (default) is the local cluster in containers, `ENV=aws`
the EC2 cluster. The same script, `bench/run_all.sh`, does the work on the master node in both cases:
build the jar, generate the datasets, codec micro-benchmark, then the experiments of
`bench/experiments-<env>.yaml` (steps already done are skipped).

| | Local | AWS |
|---|---|---|
| Start the cluster | `make up` | `make up ENV=aws` (Terraform + Ansible) |
| Run everything | `make run` | `make run ENV=aws` (in tmux on the master, the laptop can be turned off) |
| Progress | `make status` | `make status ENV=aws` |
| Get the results | `make fetch` (nothing to do) | `make fetch ENV=aws` |
| Figures and tables | `make analysis` | `make analysis ENV=aws` |
| Shell on the master | `make shell` | `make shell ENV=aws` (also opens the web UIs on localhost) |
| Stop | `make down` | `make down ENV=aws` (deletes the EC2 cluster) |

`make analysis` creates the Python environment (`.venv`) the first time. `make up ENV=aws` creates
the SSH key `~/.ssh/cloud-bigdata-project` if needed and only allows SSH from your current public IP.

Web UIs: HDFS http://localhost:9870, Spark http://localhost:8080, History Server http://localhost:18080.

Before the first `make up ENV=aws`: an AWS account with 16 vCPU of quota, `aws configure`,
`brew install terraform ansible`. Checklist and cost: [docs/AWS.md](docs/AWS.md).
