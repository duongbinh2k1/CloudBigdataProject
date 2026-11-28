"""Runs all the experiments and adds one line per run to results/runs.csv.
On the master, in /opt/work: python3 bench/run_experiments.py   (runs already in runs.csv are skipped)

For each run: empty the page cache and start `sar` on every machine, spark-submit the application,
stop `sar`, then read the metrics of the run from the Spark History Server and from `sar`.
"""
import csv
import json
import random
import subprocess
import time
import urllib.request
from pathlib import Path

# ---- Design of the experiments (the sizes and storage codecs are also in run_all.sh) ----
SIZES = [1, 5, 10, 20]                    # scale factor: SF 1 = about 1 GB of text
APPS = ["io_bound", "cpu_bound", "balanced"]
CONFIGURATIONS = [                        # (storage codec, shuffle codec)
    ("lz4", "off"),                       # E1: storage compression only
    ("none", "lz4"),                      # E2: shuffle compression only (= C2)
    ("none", "off"),                      # no compression (= C0)
    ("none", "zstd"),                     # E2
    ("zstd", "lz4"),                      # E3: both levels (= C3)
    ("zstd", "off"),                      # E1 (= C1)
]
REPETITIONS = 3
SEED = 20261111                           # random order of the runs, the same every time

RESULTS = Path("results")
JAR = "java/build/app.jar"
CLASSES = {"io_bound": "compression.IoBound", "cpu_bound": "compression.CpuBound", "balanced": "compression.Balanced"}
HISTORY = "http://master:18080/api/v1/applications/"
NODES = ["master"] + Path("/opt/spark/conf/workers").read_text().split()
COLUMNS = ["run_id", "status", "app", "sf", "storage", "shuffle", "rep", "time_s", "executor_run_ms",
           "executor_cpu_ns", "shuffle_write_bytes", "shuffle_write_ns", "shuffle_fetch_wait_ms", "cpu_busy_pct",
           "cpu_iowait_pct", "mem_used_pct", "disk_read_bytes", "disk_write_bytes", "net_rx_bytes"]


def on_node(node, command):
    """Runs a shell command on a machine of the cluster, through SSH."""
    return subprocess.run(["ssh", node, command], capture_output=True, text=True).stdout


def spark_submit(app, sf, storage, shuffle, output):
    command = ["spark-submit", "--class", CLASSES[app]]
    if shuffle == "off":
        command += ["--conf", "spark.shuffle.compress=false", "--conf", "spark.shuffle.spill.compress=false"]
    else:   # COMMUNICATION COMPRESSION with lz4 or zstd
        command += ["--conf", "spark.shuffle.compress=true", "--conf", "spark.shuffle.spill.compress=true",
                    "--conf", "spark.io.compression.codec=" + shuffle]
    data = "hdfs://master:9000/data/sf" + str(sf) + "/" + storage      # STORAGE COMPRESSION: input files
    command += [JAR, data + "/lineitem"]
    if app == "balanced":
        command += [data + "/orders"]
    return command + [output, storage]


def get_json(url):
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def spark_metrics(app_id, row):
    """Adds the metrics of the History Server to row (sum over the stages of the application)."""
    completed = False
    for attempt in range(90):      # the History Server reads the event log a few seconds after the end
        try:
            app = get_json(HISTORY + app_id)
            completed = app["attempts"][-1]["completed"]
        except Exception:
            pass
        if completed:
            break
        time.sleep(2)
    if not completed:
        raise RuntimeError("no metrics for " + app_id + " in the History Server")
    row["time_s"] = app["attempts"][-1]["duration"] / 1000
    row["executor_run_ms"] = row["executor_cpu_ns"] = 0
    row["shuffle_write_bytes"] = row["shuffle_write_ns"] = row["shuffle_fetch_wait_ms"] = 0
    for stage in get_json(HISTORY + app_id + "/stages"):
        if stage["status"] == "COMPLETE":
            row["executor_run_ms"] += stage["executorRunTime"]           # time spent by the tasks
            row["executor_cpu_ns"] += stage["executorCpuTime"]           # CPU time (includes compression)
            row["shuffle_write_bytes"] += stage["shuffleWriteBytes"]     # shuffle data (compressed size)
            row["shuffle_write_ns"] += stage["shuffleWriteTime"]         # shuffle time = write time
            row["shuffle_fetch_wait_ms"] += stage["shuffleFetchWaitTime"]   # + time waiting for remote data


def mean(values):
    if len(values) == 0:
        return ""
    return round(sum(values) / len(values), 2)


def sar_metrics(sar_output, row):
    """Adds the sar metrics to row. sar_output: machine -> output of `sadf -d` (one line per second).
    CPU and memory: mean over the machines. Disk and network: bytes summed over the machines."""
    busy, iowait, memory = [], [], []
    disk_read = disk_write = net_received = 0
    for text in sar_output.values():
        header = []
        for line in text.splitlines():
            if line.startswith("#"):             # each report (-u, -r, -b, -n DEV) starts with its header
                header = line[1:].strip().split(";")
                continue
            values = dict(zip(header, line.split(";")))
            if "%user" in values:
                busy.append(float(values["%user"]) + float(values["%nice"]) + float(values["%system"]))
                iowait.append(float(values["%iowait"]))
            elif "%memused" in values:
                memory.append(float(values["%memused"]))
            elif "bread/s" in values:            # blocks of 512 bytes per second
                disk_read += float(values["bread/s"]) * 512
                disk_write += float(values["bwrtn/s"]) * 512
            elif "IFACE" in values and values["IFACE"] != "lo":
                net_received += float(values["rxkB/s"]) * 1024
    row["cpu_busy_pct"] = mean(busy)
    row["cpu_iowait_pct"] = mean(iowait)
    row["mem_used_pct"] = mean(memory)
    row["disk_read_bytes"] = int(disk_read)
    row["disk_write_bytes"] = int(disk_write)
    row["net_rx_bytes"] = int(net_received)


def run_once(app, sf, storage, shuffle, run_id):
    for node in NODES:   # empty the page cache (data really read from disk), then 1 sar sample per second
        on_node(node, "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; "
                      "rm -f /tmp/sar.bin; nohup sar -o /tmp/sar.bin 1 >/dev/null 2>&1 &")
    output = "hdfs://master:9000/output/" + run_id
    job = subprocess.run(spark_submit(app, sf, storage, shuffle, output), capture_output=True, text=True)
    sar_output = {}
    for node in NODES:
        sar_output[node] = on_node(node, "pkill -INT -x sar; sleep 1; sadf -d /tmp/sar.bin -- -u -r -b -n DEV")

    folder = RESULTS / "sar" / run_id               # raw measurements of every machine
    folder.mkdir(parents=True, exist_ok=True)
    for node in NODES:
        (folder / (node + ".csv")).write_text(sar_output[node])

    row = {"run_id": run_id, "app": app, "sf": sf, "storage": storage, "shuffle": shuffle, "status": "failed"}
    app_id = None
    for line in job.stdout.splitlines():         # the application prints APP_ID=...
        if line.startswith("APP_ID="):
            app_id = line[len("APP_ID="):]
    if job.returncode != 0 or app_id is None:
        (RESULTS / "logs").mkdir(exist_ok=True)
        (RESULTS / "logs" / (run_id + ".log")).write_text(job.stderr)
        return row
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", output], capture_output=True)
    row["status"] = "ok"
    spark_metrics(app_id, row)
    sar_metrics(sar_output, row)
    return row


def main():
    runs_csv = RESULTS / "runs.csv"
    done = []
    if runs_csv.exists():
        with open(runs_csv) as f:
            for row in csv.DictReader(f):
                if row["status"] == "ok":
                    done.append(row["run_id"])
    else:
        RESULTS.mkdir(exist_ok=True)
        runs_csv.write_text(",".join(COLUMNS) + "\n")

    # every (application, size, configuration, repetition), in a random order fixed by the seed,
    # so that a slow period of the cluster does not always fall on the same codec
    runs = []
    for app in APPS:
        for sf in SIZES:
            for storage, shuffle in CONFIGURATIONS:
                for rep in range(1, REPETITIONS + 1):
                    runs.append((app, sf, storage, shuffle, rep))
    random.Random(SEED).shuffle(runs)
    todo = []
    for app, sf, storage, shuffle, rep in runs:
        run_id = app + "_sf" + str(sf) + "_" + storage + "_" + shuffle + "_r" + str(rep)
        if run_id not in done:
            todo.append((app, sf, storage, shuffle, rep, run_id))
    print(len(done), "runs done,", len(todo), "to run", flush=True)
    if not todo:
        return

    for app in APPS:        # one warm-up run per application (JVM, HDFS), not recorded
        run_once(app, SIZES[0], "none", "off", "warmup_" + app)

    for i, (app, sf, storage, shuffle, rep, run_id) in enumerate(todo, 1):
        row = run_once(app, sf, storage, shuffle, run_id)
        row["rep"] = rep
        with open(runs_csv, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n").writerow(row)
        print("[" + str(i) + "/" + str(len(todo)) + "]", run_id, row["status"], row.get("time_s", ""), flush=True)


if __name__ == "__main__":
    main()
