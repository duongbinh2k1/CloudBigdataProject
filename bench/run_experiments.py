"""Runs every experiment of bench/experiments.yaml and adds one line per run to results/runs.csv.
On the master, in /opt/work: python3 bench/run_experiments.py   (runs already in runs.csv are skipped)

For each run: drop the page cache and start `sar` on every node, spark-submit the app, stop `sar`,
then read the metrics of the run from the Spark History Server and from `sar`.
"""
import csv
import json
import random
import re
import subprocess
import time
import urllib.request
from pathlib import Path

import yaml

CONFIG = yaml.safe_load(Path("bench/experiments.yaml").read_text())
RESULTS = Path("results")
JAR = "java/build/app.jar"
DATA = "hdfs://master:9000/data"
OUTPUT = "hdfs://master:9000/output"
HISTORY = "http://master:18080/api/v1/applications"
NODES = ["master"] + Path("/opt/spark/conf/workers").read_text().split()
CLASSES = {"io_bound": "compression.IoBound", "cpu_bound": "compression.CpuBound", "balanced": "compression.Balanced"}

# metric in the History Server (sum over the stages of the app) -> column of runs.csv
SPARK_METRICS = {
    "executorRunTime": "executor_run_ms",        # time spent by the tasks
    "executorCpuTime": "executor_cpu_ns",        # CPU time of the tasks (includes compression work)
    "shuffleWriteBytes": "shuffle_write_bytes",  # shuffle data written by the map tasks (compressed size)
    "shuffleWriteTime": "shuffle_write_ns",      # shuffle time = write time + fetch wait time
    "shuffleFetchWaitTime": "shuffle_fetch_wait_ms",
}
SAR_METRICS = ["cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "disk_read_bytes", "disk_write_bytes",
               "net_rx_bytes"]
COLUMNS = (["run_id", "status", "app", "sf", "storage", "shuffle", "rep", "time_s"]
           + list(SPARK_METRICS.values()) + SAR_METRICS)


def configurations():
    """(storage codec, shuffle codec) pairs: E1, E2 and C0-C3 of E3 (each pair is run only once)."""
    pairs = {(codec, "off") for codec in CONFIG["storage_codecs"]}           # E1: storage only
    pairs |= {("none", codec) for codec in CONFIG["shuffle_codecs"]}         # E2: shuffle only
    x, y = CONFIG["combined"]["storage"], CONFIG["combined"]["shuffle"]      # E3: C0, C1, C2, C3
    pairs |= {("none", "off"), (x, "off"), ("none", y), (x, y)}
    return sorted(pairs)


def spark_submit(app, sf, storage, shuffle, output):
    if shuffle == "off":
        conf = ["spark.shuffle.compress=false", "spark.shuffle.spill.compress=false"]
    else:      # COMMUNICATION COMPRESSION: lz4 or zstd
        conf = ["spark.shuffle.compress=true", "spark.shuffle.spill.compress=true",
                f"spark.io.compression.codec={shuffle}"]
    data = f"{DATA}/sf{sf}/{storage}"
    inputs = [f"{data}/lineitem", f"{data}/orders"] if app == "balanced" else [f"{data}/lineitem"]
    command = ["spark-submit", "--class", CLASSES[app]]
    for setting in conf:
        command += ["--conf", setting]
    return command + [JAR] + inputs + [output, storage]


def on_node(node, command):
    return subprocess.run(["ssh", node, command], capture_output=True, text=True).stdout


def spark_metrics(app_id):
    url = f"{HISTORY}/{app_id}"
    for _ in range(90):           # the History Server reads the event log a few seconds after the end
        try:
            with urllib.request.urlopen(url) as response:
                app = json.load(response)
            if app["attempts"][-1]["completed"]:
                break
        except (OSError, ValueError):
            pass
        time.sleep(2)
    else:
        raise TimeoutError(f"no metrics for {app_id} in the History Server")
    with urllib.request.urlopen(url + "/stages") as response:
        stages = [s for s in json.load(response) if s["status"] == "COMPLETE"]
    row = {"time_s": app["attempts"][-1]["duration"] / 1000}
    for field, column in SPARK_METRICS.items():
        row[column] = sum(stage[field] for stage in stages)
    return row


def sar_metrics(sar_csv):
    """sar_csv: node -> output of `sadf -d` (one line per second). CPU and memory: mean over the nodes;
    disk and network: bytes summed over the nodes (rate per second x 1 second)."""
    busy, iowait, mem = [], [], []
    total = {"disk_read_bytes": 0, "disk_write_bytes": 0, "net_rx_bytes": 0}
    for text in sar_csv.values():
        header = []
        for line in text.splitlines():
            if line.startswith("#"):                 # each report (-u, -r, -b, -n DEV) has its header
                header = line[1:].strip().split(";")
                continue
            r = dict(zip(header, line.split(";")))
            if "%user" in r:
                busy.append(float(r["%user"]) + float(r["%nice"]) + float(r["%system"]))
                iowait.append(float(r["%iowait"]))
            elif "%memused" in r:
                mem.append(float(r["%memused"]))
            elif "bread/s" in r:                     # blocks of 512 bytes
                total["disk_read_bytes"] += float(r["bread/s"]) * 512
                total["disk_write_bytes"] += float(r["bwrtn/s"]) * 512
            elif r.get("IFACE", "lo") != "lo":
                total["net_rx_bytes"] += float(r["rxkB/s"]) * 1024

    def mean(values):
        return round(sum(values) / len(values), 2) if values else ""

    return {"cpu_busy_pct": mean(busy), "cpu_iowait_pct": mean(iowait), "mem_used_pct": mean(mem),
            **{k: int(v) for k, v in total.items()}}


def run_once(app, sf, storage, shuffle, run_id):
    for node in NODES:   # empty the page cache (data really read from disk), then 1 sar sample per second
        on_node(node, "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; "
                      "rm -f /tmp/sar.bin; nohup sar -o /tmp/sar.bin 1 >/dev/null 2>&1 &")
    output = f"{OUTPUT}/{run_id}"
    proc = subprocess.run(spark_submit(app, sf, storage, shuffle, output), capture_output=True, text=True)
    sar_csv = {node: on_node(node, "pkill -INT -x sar; sleep 1; sadf -d /tmp/sar.bin -- -u -r -b -n DEV")
               for node in NODES}

    raw = RESULTS / "sar" / run_id                  # raw measurements, kept for the report
    raw.mkdir(parents=True, exist_ok=True)
    for node, text in sar_csv.items():
        (raw / f"{node}.csv").write_text(text)

    row = {"run_id": run_id, "app": app, "sf": sf, "storage": storage, "shuffle": shuffle}
    app_id = re.search(r"APP_ID=(\S+)", proc.stdout)
    if proc.returncode != 0 or not app_id:
        (RESULTS / "logs").mkdir(exist_ok=True)
        (RESULTS / "logs" / f"{run_id}.log").write_text(proc.stderr)
        return {**row, "status": "failed"}
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", output], capture_output=True)
    return {**row, "status": "ok", **spark_metrics(app_id.group(1)), **sar_metrics(sar_csv)}


def main():
    runs_csv = RESULTS / "runs.csv"
    done = set()
    if runs_csv.exists():
        with runs_csv.open() as f:
            done = {r["run_id"] for r in csv.DictReader(f) if r["status"] == "ok"}
    else:
        RESULTS.mkdir(exist_ok=True)
        runs_csv.write_text(",".join(COLUMNS) + "\n")

    # every (app, size, configuration, repetition), in a random order fixed by the seed, so that a slow
    # period of the cluster does not always fall on the same codec
    runs = [(app, sf, storage, shuffle, rep) for app in CONFIG["apps"] for sf in CONFIG["sizes"]
            for storage, shuffle in configurations() for rep in range(1, CONFIG["repetitions"] + 1)]
    random.Random(CONFIG["seed"]).shuffle(runs)
    todo = [r for r in runs if "{}_sf{}_{}_{}_r{}".format(*r) not in done]
    print(f"{len(done)} runs done, {len(todo)} to run", flush=True)

    for app in CONFIG["apps"]:        # one warm-up run per app (JVM, HDFS), not recorded
        if todo:
            run_once(app, CONFIG["sizes"][0], "none", "off", f"warmup_{app}")

    for i, (app, sf, storage, shuffle, rep) in enumerate(todo, 1):
        run_id = f"{app}_sf{sf}_{storage}_{shuffle}_r{rep}"
        row = {**run_once(app, sf, storage, shuffle, run_id), "rep": rep}
        with runs_csv.open("a", newline="") as f:
            csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n").writerow(row)
        print(f"[{i}/{len(todo)}] {run_id}: {row['status']} {row.get('time_s', '')}", flush=True)


if __name__ == "__main__":
    main()
