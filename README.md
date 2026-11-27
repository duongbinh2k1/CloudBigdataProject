# Experimental Evaluation of Data Compression in Spark

InfraCloud project (USTH / INP-ENSEEIHT). We measure the gain and the cost of compression in Spark
at two levels:

- **storage**: text files on HDFS compressed with a Hadoop codec,
- **communication**: shuffle data compressed by Spark,

with three applications (I/O-bound, CPU-bound, balanced) and four dataset sizes (1, 5, 10, 20 GB),
on a cluster of 8 EC2 machines (1 master + 7 workers).

Codecs: two of the four codecs of the assignment, **LZ4** (fast, ratio ~2) and **Zstd** (ratio ~3, more CPU),
both available for files and for the shuffle, against no compression. Their intrinsic characteristics are
measured on one machine by `bench/CodecBench.java`.

Software: Java 11, Hadoop 3.4.1 (HDFS only), Spark 3.5.5 standalone, Spark jobs in Java (RDD API).
Tools: Terraform (machines), Ansible (installation), `sar` + Spark History Server (measurements),
Python (experiment script, figures).

| Folder | Content |
|---|---|
| `infra/terraform/` | the AWS machines: network, firewall, 1 master + 7 workers `m7i-flex.large` |
| `infra/ansible/` | `cluster.yml`: installs and starts HDFS + Spark on the machines |
| `cluster/` | installation script, Hadoop and Spark configuration, start script |
| `java/src/compression/` | data generator (`DataGen`), the 3 apps (`IoBound`, `CpuBound`, `Balanced`), `Util` |
| `bench/` | `experiments.yaml` (design), `run_all.sh` (whole study), `run_experiments.py` (runs + metrics), `CodecBench.java` |
| `analysis/` | `report.py`: figures and tables |
| `results/` | raw measurements: `runs.csv`, `sar.tar.gz` (raw `sar` data, `tar xzf results/sar.tar.gz -C results`), `datasets/`, `codec_bench.csv`, `run.log` |
| `docs/GIAI-THICH.md` | full explanation of the project and defence questions (Vietnamese) |

## Requirements (once)

- An AWS account with 16 vCPU of quota in Sydney (`ap-southeast-2`), and an IAM user's access key:
  `aws configure` (region `ap-southeast-2`).
- On the laptop: `brew install terraform ansible awscli`, and Python 3.

## Run

All commands run on the laptop, in this folder.

| Command | What it does |
|---|---|
| `make up` | Terraform creates the machines, Ansible installs and starts HDFS + Spark (~15 min) |
| `make run` | starts the whole study on the master in `tmux` (build, datasets, codec benchmark, 216 runs; about 14-17 h). The laptop can be turned off |
| `make status` | last lines of the log and number of runs done |
| `make fetch` | copies the results of the master to `results/` (do it regularly) |
| `make analysis` | figures in `analysis/figures/`, tables in `analysis/tables/` |
| `make shell` | shell on the master; web UIs: HDFS http://localhost:9870, Spark http://localhost:8080, History Server http://localhost:18080 |
| `make down` | deletes every AWS resource. **The cluster costs about 1.1 USD per hour, even when idle** |

If the run stops, `make run` again: the runs already in `results/runs.csv` are skipped.
After a change in the code or in `bench/experiments.yaml`: `make up` (copies the code again), then `make run`.
`make up` restarts HDFS and Spark: never run it while a study is running.
