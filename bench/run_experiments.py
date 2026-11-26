"""Runs the experiments of a YAML file and writes one line per run in <results>/runs.csv.

    python3 bench/run_experiments.py bench/experiments-dev.yaml results/dev

Runs already "ok" in runs.csv are skipped, so the same command continues after an interruption.
"""
import argparse
import csv
import random
import subprocess
import time
from pathlib import Path

import yaml

import metrics

JAR = "java/build/cloud-bigdata-project.jar"
DATA = "hdfs://master:9000/data"
OUTPUT = "hdfs://master:9000/results"
HISTORY = "http://master:18080"
WORKERS_FILE = "/opt/spark/conf/workers"   # written by Ansible on EC2, absent in the dev containers
CLASSES = {"io_bound": "compression.apps.IoBound",
           "cpu_bound": "compression.apps.CpuBound",
           "balanced": "compression.apps.Balanced"}
COLUMNS = (["run_id", "status", "app", "sf", "storage", "shuffle", "rep", "wall_s", "output_hdfs_bytes"]
           + metrics.SPARK_COLUMNS + metrics.SAR_COLUMNS)


def configurations(cfg):
    # (storage codec, shuffle codec) pairs of E1, E2 and E3, each only once
    pairs = {(s, "off") for s in cfg["storage_codecs"]}            # E1
    pairs |= {("none", s) for s in cfg["shuffle_codecs"]}          # E2
    if cfg.get("combined"):                                        # E3
        x, y = cfg["combined"]["storage"], cfg["combined"]["shuffle"]
        pairs |= {("none", "off"), (x, "off"), ("none", y), (x, y)}
    return pairs


def run_list(cfg):
    runs = [(app, sf, storage, shuffle, rep)
            for app in cfg["apps"]
            for sf in cfg["sizes"]
            for storage, shuffle in sorted(configurations(cfg))
            for rep in range(1, cfg["repetitions"] + 1)]
    random.Random(cfg["seed"]).shuffle(runs)   # random order, but always the same one
    return runs


def run_id(app, sf, storage, shuffle, rep):
    return f"{app}_sf{sf:g}_{storage}_{shuffle}_r{rep}"


def submit_command(cfg, app, sf, storage, shuffle, output):
    if shuffle == "off":
        shuffle_conf = {"spark.shuffle.compress": "false", "spark.shuffle.spill.compress": "false"}
    else:   # lz4, snappy, zstd (Spark has no gzip for shuffle)
        shuffle_conf = {"spark.shuffle.compress": "true", "spark.shuffle.spill.compress": "true",
                        "spark.io.compression.codec": shuffle}
    # everything else: Spark's defaults and conf/spark/spark-defaults.conf
    cmd = ["spark-submit", "--class", CLASSES[app]]
    for key, value in shuffle_conf.items():
        cmd += ["--conf", f"{key}={value}"]
    data = f"{DATA}/sf{sf:g}/{storage}"
    if app == "balanced":
        args = [f"{data}/lineitem", f"{data}/orders", output, storage]
    else:
        args = [f"{data}/lineitem", output, storage]
    rounds = cfg.get("rounds", {}).get(app)
    return cmd + [JAR] + args + ([str(rounds)] if rounds else [])


def cluster_nodes():
    # EC2: the master and the workers listed for start-workers.sh, reached by ssh.
    # Dev: everything runs in the master container.
    if Path(WORKERS_FILE).exists():
        return ["master"] + Path(WORKERS_FILE).read_text().split(), True
    return ["master"], False


NODES, USE_SSH = cluster_nodes()


def on_node(node, command):
    argv = ["ssh", node, command] if USE_SSH else ["sh", "-c", command]
    return subprocess.run(argv, capture_output=True, text=True)


def run_once(cfg, app, sf, storage, shuffle, name, out_dir):
    output = f"{OUTPUT}/{name}"
    sar_file = f"/tmp/sar-{name}.bin"
    for node in NODES:
        if on_node(node, "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches").returncode != 0:
            print(f"  warning: could not drop the page cache on {node}")
        on_node(node, f"nohup sar -o {sar_file} 1 >/dev/null 2>&1 &")

    start = time.time()
    proc = subprocess.run(submit_command(cfg, app, sf, storage, shuffle, output), capture_output=True, text=True)
    wall_s = round(time.time() - start, 3)

    sar_texts = {}
    for node in NODES:
        on_node(node, "pkill -INT -x sar; sleep 1")
        sar_texts[node] = {kind: on_node(node, f"sadf -d {sar_file} -- {flags}").stdout
                           for kind, flags in metrics.SAR_REPORTS.items()}
        on_node(node, f"rm -f {sar_file}")
        sar_dir = out_dir / "sar" / name
        sar_dir.mkdir(parents=True, exist_ok=True)
        for kind, text in sar_texts[node].items():
            (sar_dir / f"{node}-{kind}.csv").write_text(text)

    row = {"run_id": name, "app": app, "sf": sf, "storage": storage, "shuffle": shuffle, "wall_s": wall_s}
    if proc.returncode != 0:
        (out_dir / "logs").mkdir(exist_ok=True)
        (out_dir / "logs" / f"{name}.log").write_text(proc.stderr)
        return {**row, "status": "failed"}

    app_id = next(line.split("=")[1] for line in proc.stdout.splitlines() if line.startswith("APP_ID="))
    du = subprocess.run(["hdfs", "dfs", "-du", "-s", output], capture_output=True, text=True).stdout
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", output], capture_output=True)
    return {**row, "status": "ok", "output_hdfs_bytes": int(du.split()[0]),
            **metrics.spark_metrics(HISTORY, app_id), **metrics.sar_summary(sar_texts)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("results", type=Path)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text())
    out_dir = args.results
    out_dir.mkdir(parents=True, exist_ok=True)
    runs_csv = out_dir / "runs.csv"
    # Adding sizes, codecs or repetitions is fine: only the new runs are done. Changing the
    # work of an app (rounds) is not: the old runs would be mixed with runs of another workload.
    old = out_dir / "config.yaml"
    if runs_csv.exists() and old.exists() and yaml.safe_load(old.read_text()).get("rounds") != cfg.get("rounds"):
        raise SystemExit(f"rounds changed since the runs in {runs_csv}: move {out_dir} away to start again")
    old.write_text(args.config.read_text())

    done = set()
    if runs_csv.exists():
        with runs_csv.open() as f:
            done = {r["run_id"] for r in csv.DictReader(f) if r["status"] == "ok"}
    else:
        with runs_csv.open("w", newline="") as f:
            csv.writer(f, lineterminator="\n").writerow(COLUMNS)

    todo = [r for r in run_list(cfg) if run_id(*r) not in done][:args.limit]
    print(f"{len(done)} runs already done, {len(todo)} to run")
    if not todo:
        return

    for app in cfg["apps"]:   # one warm-up run per app, not recorded
        sf = min(cfg["sizes"])
        name = f"warmup_{app}"
        subprocess.run(submit_command(cfg, app, sf, "none", "off", f"{OUTPUT}/{name}"), capture_output=True)
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", f"{OUTPUT}/{name}"], capture_output=True)
        print(f"warm-up {app} done")

    for i, (app, sf, storage, shuffle, rep) in enumerate(todo, 1):
        name = run_id(app, sf, storage, shuffle, rep)
        row = run_once(cfg, app, sf, storage, shuffle, name, out_dir)
        row["rep"] = rep
        with runs_csv.open("a", newline="") as f:
            csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n").writerow(row)
        print(f"[{i}/{len(todo)}] {name}: {row['status']} in {row['wall_s']} s", flush=True)
        time.sleep(3)   # let the cluster settle


if __name__ == "__main__":
    main()
