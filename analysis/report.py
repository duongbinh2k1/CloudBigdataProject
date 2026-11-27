"""Figures (analysis/figures/) and tables (analysis/tables/) from results/. Run with: make analysis

Every point or bar = mean of the 3 repetitions; error bars = standard deviation.
E1 = runs with the shuffle not compressed (storage codec varies), E2 = runs with the files not compressed.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import yaml

RESULTS = Path("results")
FIGURES = Path("analysis/figures")
TABLES = Path("analysis/tables")
APPS = {"io_bound": "I/O-bound", "balanced": "Balanced", "cpu_bound": "CPU-bound"}
COLORS = {"none": "gray", "off": "gray", "lz4": "royalblue", "zstd": "orangered", "zstd-1": "orangered",
          "zstd-3": "darkred"}
plt.rcParams.update({"axes.grid": True, "font.size": 8, "axes.titlesize": 9, "legend.fontsize": 7})


def load_runs():
    runs = pd.read_csv(RESULTS / "runs.csv")
    runs = runs[runs["status"] == "ok"].copy()
    runs["cpu_s"] = runs["executor_cpu_ns"] / 1e9
    runs["disk_gb"] = (runs["disk_read_bytes"] + runs["disk_write_bytes"]) / 1e9
    runs["net_gb"] = runs["net_rx_bytes"] / 1e9
    runs["disk_mb_s"] = runs["disk_gb"] * 1000 / runs["time_s"]           # throughputs, all machines
    runs["net_mb_s"] = runs["net_gb"] * 1000 / runs["time_s"]
    runs["shuffle_gb"] = runs["shuffle_write_bytes"] / 1e9
    runs["shuffle_s"] = runs["shuffle_write_ns"] / 1e9 + runs["shuffle_fetch_wait_ms"] / 1e3
    return runs


def e1(runs):
    return runs[runs["shuffle"] == "off"]      # storage compression only


def e2(runs):
    return runs[runs["storage"] == "none"]     # shuffle compression only


def save(fig, name):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES / f"{name}.png", dpi=150)
    plt.close(fig)
    print("  ", FIGURES / f"{name}.png")


def lines(ax, runs, level, metric, title, ylabel):
    """One line per codec: mean and standard deviation of `metric` vs dataset size."""
    s = runs.groupby(["sf", level])[metric].agg(["mean", "std"]).fillna(0).reset_index()
    for codec, r in s.groupby(level):
        ax.errorbar(r["sf"], r["mean"], yerr=r["std"], label=codec, color=COLORS[codec], marker="o", capsize=3)
    ax.set_title(title)
    ax.set_xlabel("Dataset size (SF, about GB)")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)


def vs_size(runs, metric, ylabel, name):
    """Figures 1-2: metric vs dataset size. Row 1 = storage compression (E1), row 2 = shuffle compression (E2)."""
    fig, axes = plt.subplots(2, 3, figsize=(10, 5))
    for row, (level, part) in enumerate([("storage", e1(runs)), ("shuffle", e2(runs))]):
        for ax, (app, title) in zip(axes[row], APPS.items()):
            lines(ax, part[part["app"] == app], level, metric, f"{title}, {level} compression", ylabel)
        axes[row][0].legend()
    save(fig, name)


def disk_network(runs):
    """Figure 3: disk volume with storage compression (row 1), network volume with shuffle compression (row 2)."""
    fig, axes = plt.subplots(2, 3, figsize=(10, 5))
    for ax, (app, title) in zip(axes[0], APPS.items()):
        lines(ax, e1(runs)[e1(runs)["app"] == app], "storage", "disk_gb", f"{title}, storage compression",
              "Disk read + write (GB)")
    for ax, (app, title) in zip(axes[1], APPS.items()):
        lines(ax, e2(runs)[e2(runs)["app"] == app], "shuffle", "net_gb", f"{title}, shuffle compression",
              "Network received (GB)")
    axes[0][0].legend()
    axes[1][0].legend()
    save(fig, "fig3_disk_network_vs_size")


def compression_ratio(runs):
    """Figure 4: compression ratio (size without compression / size with the codec)."""
    fig, axes = plt.subplots(1, 3, figsize=(10, 2.8))
    bench = pd.read_csv(RESULTS / "codec_bench.csv").groupby("codec")[["input_bytes", "compressed_bytes"]].mean()
    ratio = bench["input_bytes"] / bench["compressed_bytes"]
    axes[0].bar(ratio.index, ratio, color=[COLORS[c] for c in ratio.index])
    axes[0].set_title("Micro-benchmark (one 75 MB file)")

    sizes = []        # results/datasets/sf*.txt: output of hdfs dfs -du (size, size with replicas, path)
    for f in RESULTS.glob("datasets/sf*.txt"):
        for line in f.read_text().splitlines():
            size, _, path = line.split()
            sizes.append({"sf": f.stem, "codec": path.split("/")[-2], "bytes": int(size)})
    per_codec = pd.DataFrame(sizes).groupby(["sf", "codec"])["bytes"].sum().unstack()
    ratio = per_codec.rdiv(per_codec["none"], axis=0).mean().drop("none")
    axes[1].bar(ratio.index, ratio, color=[COLORS[c] for c in ratio.index])
    axes[1].set_title("Files on HDFS (level 3 for Zstd)")

    shuffle = e2(runs).groupby(["app", "shuffle"])["shuffle_write_bytes"].mean().unstack()
    ratio = shuffle.rdiv(shuffle["off"], axis=0).drop(columns="off").drop(index="cpu_bound")   # its shuffle: ~0
    ratio.index = [APPS[a] for a in ratio.index]
    ratio.plot.bar(ax=axes[2], color=[COLORS[c] for c in ratio.columns], rot=0)
    axes[2].set_title("Shuffle data (level 1 for Zstd)")
    for ax in axes:
        ax.set_ylabel("Compression ratio")
    save(fig, "fig4_compression_ratio")


def configurations(runs, combined):
    """Figure 5: execution time of C0-C3 for each app, at the largest size."""
    x, y = combined["storage"], combined["shuffle"]
    names = {("none", "off"): "C0", (x, "off"): "C1", ("none", y): "C2", (x, y): "C3"}
    runs = runs.assign(config=[names.get(k) for k in zip(runs["storage"], runs["shuffle"])]).dropna(subset=["config"])
    sf = runs["sf"].max()
    s = runs[runs["sf"] == sf].groupby(["app", "config"])["time_s"].agg(["mean", "std"]).reset_index()
    fig, axes = plt.subplots(1, 3, figsize=(10, 2.8))
    for ax, (app, title) in zip(axes, APPS.items()):
        r = s[s["app"] == app].sort_values("config")
        ax.bar(r["config"], r["mean"], yerr=r["std"], capsize=3, color=["gray", "orangered", "royalblue", "purple"])
        ax.set_title(title)
        ax.set_ylabel("Execution time (s)")
    fig.suptitle(f"SF {sf}: C0 none/off, C1 {x}/off, C2 none/{y}, C3 {x}/{y} (storage/shuffle)", fontsize=9)
    save(fig, "fig5_configurations")


def workload(runs):
    """Figure 6: % faster than without compression, for each app (workload type), at the largest size."""
    sf = runs["sf"].max()
    fig, axes = plt.subplots(1, 2, figsize=(10, 2.8))
    for ax, level, baseline, part in ((axes[0], "storage", "none", e1(runs)), (axes[1], "shuffle", "off", e2(runs))):
        times = part[part["sf"] == sf].groupby(["app", level])["time_s"]
        mean, std = times.mean().unstack(), times.std().unstack()
        gain = mean.rsub(mean[baseline], axis=0).div(mean[baseline], axis=0).drop(columns=baseline) * 100
        error = std.div(mean[baseline], axis=0).drop(columns=baseline) * 100
        gain.index = error.index = [APPS[a] for a in gain.index]
        gain.plot.bar(ax=ax, yerr=error, capsize=3, color=[COLORS[c] for c in gain.columns], rot=0)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(f"{level.capitalize()} compression, SF {sf}")
        ax.set_ylabel("Faster than no compression (%)")
    save(fig, "fig6_workload_type")


def tables(runs):
    """Codec micro-benchmark, workload characterisation (no compression) and all the means."""
    TABLES.mkdir(parents=True, exist_ok=True)
    bench = pd.read_csv(RESULTS / "codec_bench.csv")
    bench["ratio"] = bench["input_bytes"] / bench["compressed_bytes"]
    bench["compress_mb_s"] = bench["input_bytes"] / bench["compress_s"] / 1e6
    bench["decompress_mb_s"] = bench["input_bytes"] / bench["decompress_s"] / 1e6
    columns = ["ratio", "compress_s", "decompress_s", "compress_mb_s", "decompress_mb_s"]
    bench.groupby("codec")[columns].median().round(3).to_csv(TABLES / "codec_bench.csv")

    c0 = runs[(runs["storage"] == "none") & (runs["shuffle"] == "off")].copy()
    c0["cpu_share"] = c0["executor_cpu_ns"] / 1e6 / c0["executor_run_ms"]    # CPU time / task time
    columns = ["time_s", "cpu_share", "cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "disk_gb", "net_gb",
               "shuffle_gb"]
    c0.groupby(["app", "sf"])[columns].mean().round(3).to_csv(TABLES / "workload_characterisation.csv")

    metrics = ["time_s", "cpu_s", "cpu_busy_pct", "disk_gb", "disk_mb_s", "net_gb", "net_mb_s", "shuffle_gb",
               "shuffle_s"]
    runs.groupby(["app", "sf", "storage", "shuffle"])[metrics].agg(["mean", "std"]).round(3) \
        .to_csv(TABLES / "summary.csv")
    print("   tables in", TABLES)


def main():
    runs = load_runs()
    vs_size(runs, "time_s", "Execution time (s)", "fig1_time_vs_size")
    vs_size(runs, "cpu_s", "CPU time of the executors (s)", "fig2_cpu_vs_size")
    disk_network(runs)
    compression_ratio(runs)
    configurations(runs, yaml.safe_load(Path("bench/experiments.yaml").read_text())["combined"])
    workload(runs)
    tables(runs)


if __name__ == "__main__":
    main()
