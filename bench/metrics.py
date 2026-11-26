"""Metrics of one run: Spark History Server (REST API) and sar."""
import json
import time
import urllib.request

# stage field in the History Server -> column in runs.csv (times are in ms, or in ns for *_ns)
STAGE_FIELDS = {
    "executorRunTime": "executor_run_ms",
    "executorCpuTime": "executor_cpu_ns",
    "jvmGcTime": "gc_ms",
    "inputBytes": "input_bytes",
    "outputBytes": "output_bytes",
    "shuffleReadBytes": "shuffle_read_bytes",
    "shuffleRemoteBytesRead": "shuffle_remote_read_bytes",
    "shuffleWriteBytes": "shuffle_write_bytes",
    "shuffleFetchWaitTime": "shuffle_fetch_wait_ms",
    "shuffleWriteTime": "shuffle_write_ns",
    "diskBytesSpilled": "disk_spill_bytes",
    "memoryBytesSpilled": "memory_spill_bytes",
}
SPARK_COLUMNS = ["app_id", "duration_ms"] + list(STAGE_FIELDS.values())
SAR_COLUMNS = ["cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "mem_max_pct", "disk_read_bytes",
               "disk_write_bytes", "net_rx_bytes", "net_tx_bytes"]
SAR_REPORTS = {"cpu": "-u", "mem": "-r", "disk": "-b", "net": "-n DEV"}


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def spark_metrics(history_url, app_id, timeout_s=180):
    url = f"{history_url}/api/v1/applications/{app_id}"
    deadline = time.time() + timeout_s
    while True:
        try:
            app = get_json(url)
            if app["attempts"][-1]["completed"]:
                stages = get_json(url + "/stages")
                break
        except Exception:      # not yet read by the History Server
            pass
        if time.time() > deadline:
            raise TimeoutError(f"no metrics for {app_id} after {timeout_s}s")
        time.sleep(2)

    row = {"app_id": app_id, "duration_ms": app["attempts"][-1]["duration"]}
    counted = [s for s in stages if s["status"] in ("COMPLETE", "FAILED")]   # not SKIPPED
    for field, column in STAGE_FIELDS.items():
        row[column] = sum(s.get(field, 0) for s in counted)
    return row


def parse_sadf(text):
    # sadf -d output: a header line starting with '#', then values separated by ';'
    rows, header = [], None
    for line in text.splitlines():
        if line.startswith("#"):
            header = line[1:].strip().split(";")
        elif line.strip() and header:
            rows.append(dict(zip(header, line.split(";"))))
    return rows


def sar_summary(per_node):
    # CPU and memory %: mean over the nodes (memory also its peak). Disk and network: bytes summed over the nodes
    busy, iowait, mem, mem_max = [], [], [], 0.0
    totals = dict.fromkeys(SAR_COLUMNS[4:], 0)
    for texts in per_node.values():
        cpu = parse_sadf(texts["cpu"])
        if cpu:
            busy.append(sum(float(r["%user"]) + float(r["%nice"]) + float(r["%system"]) for r in cpu) / len(cpu))
            iowait.append(sum(float(r["%iowait"]) for r in cpu) / len(cpu))
        used = [float(r["%memused"]) for r in parse_sadf(texts["mem"])]
        if used:
            mem.append(sum(used) / len(used))
            mem_max = max(mem_max, *used)
        for r in parse_sadf(texts["disk"]):           # blocks of 512 bytes
            totals["disk_read_bytes"] += float(r["bread/s"]) * 512
            totals["disk_write_bytes"] += float(r["bwrtn/s"]) * 512
        for r in parse_sadf(texts["net"]):
            if r["IFACE"] != "lo":
                totals["net_rx_bytes"] += float(r["rxkB/s"]) * 1024
                totals["net_tx_bytes"] += float(r["txkB/s"]) * 1024
    summary = {k: int(v) for k, v in totals.items()}
    summary["cpu_busy_pct"] = round(sum(busy) / len(busy), 2) if busy else ""
    summary["cpu_iowait_pct"] = round(sum(iowait) / len(iowait), 2) if iowait else ""
    summary["mem_used_pct"] = round(sum(mem) / len(mem), 2) if mem else ""
    summary["mem_max_pct"] = round(mem_max, 2) if mem else ""
    return summary
