"""Figures (analysis/figures/) and tables (analysis/tables/) from results/. Run with: make analysis

Every point or bar = mean of the 3 repetitions; error bars = standard deviation.
E1 = runs with the shuffle not compressed (storage codec changes),
E2 = runs with the files not compressed (shuffle codec changes).
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

RESULTS = Path("results")
FIGURES = Path("analysis/figures")
TABLES = Path("analysis/tables")
APPS = {"io_bound": "I/O-bound", "balanced": "Balanced", "cpu_bound": "CPU-bound"}
COLORS = {"none": "gray", "off": "gray", "lz4": "royalblue", "zstd": "orangered"}
CONFIGS = {"C0": ("none", "off"), "C1": ("zstd", "off"), "C2": ("none", "lz4"), "C3": ("zstd", "lz4")}
plt.rcParams.update({"axes.grid": True, "font.size": 8, "axes.titlesize": 9, "legend.fontsize": 7})


def load_runs():
    runs = pd.read_csv(RESULTS / "runs.csv")
    runs = runs[runs["status"] == "ok"]
    runs["cpu_s"] = runs["executor_cpu_ns"] / 1e9
    runs["disk_gb"] = (runs["disk_read_bytes"] + runs["disk_write_bytes"]) / 1e9
    runs["net_gb"] = runs["net_rx_bytes"] / 1e9
    runs["disk_mb_s"] = runs["disk_gb"] * 1000 / runs["time_s"]       # throughputs, all machines
    runs["net_mb_s"] = runs["net_gb"] * 1000 / runs["time_s"]
    runs["shuffle_gb"] = runs["shuffle_write_bytes"] / 1e9
    runs["shuffle_s"] = runs["shuffle_write_ns"] / 1e9 + runs["shuffle_fetch_wait_ms"] / 1000
    return runs


def select(runs, app=None, sf=None, storage=None, shuffle=None):
    """The runs with the given application, size, storage codec and shuffle codec."""
    if app is not None:
        runs = runs[runs["app"] == app]
    if sf is not None:
        runs = runs[runs["sf"] == sf]
    if storage is not None:
        runs = runs[runs["storage"] == storage]
    if shuffle is not None:
        runs = runs[runs["shuffle"] == shuffle]
    return runs


def save(fig, name):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES / (name + ".png"), dpi=150)
    plt.close(fig)
    print("  ", FIGURES / (name + ".png"))


def plot_vs_size(ax, runs, app, level, metric, ylabel):
    """One line per codec: metric vs dataset size, for E1 (level = storage) or E2 (level = shuffle)."""
    for codec in ["none", "lz4", "zstd"]:
        if level == "storage":
            part = select(runs, app=app, storage=codec, shuffle="off")
        else:
            part = select(runs, app=app, storage="none", shuffle=("off" if codec == "none" else codec))
        stats = part.groupby("sf")[metric].agg(["mean", "std"])
        label = "no compression" if codec == "none" else codec
        ax.errorbar(stats.index, stats["mean"], yerr=stats["std"], label=label, color=COLORS[codec], marker="o",
                    capsize=3)
    ax.set_title(APPS[app] + ", " + level + " compression")
    ax.set_xlabel("Dataset size (SF, about GB)")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)


def figure_vs_size(runs, rows, name):
    """2 x 3 figure: one row per (level, metric, label), one column per application."""
    fig, axes = plt.subplots(2, 3, figsize=(10, 4.6))
    for i, (level, metric, ylabel) in enumerate(rows):
        for j, app in enumerate(APPS):
            plot_vs_size(axes[i][j], runs, app, level, metric, ylabel)
        axes[i][0].legend()
    save(fig, name)


def percent_faster(runs, app, sf, storage, shuffle):
    """Mean and std of the gain in %, compared with no compression."""
    base = select(runs, app, sf, "none", "off")["time_s"].mean()
    times = select(runs, app, sf, storage, shuffle)["time_s"]
    return (base - times.mean()) / base * 100, times.std() / base * 100


def summary_figure(runs):
    """Compression ratio of each codec, configurations C0-C3, and gain for each workload type (SF 20)."""
    fig, axes = plt.subplots(1, 3, figsize=(10, 2.9))
    sf = 20

    # 1. compression ratio = size without compression / size with the codec
    bench = pd.read_csv(RESULTS / "codec_bench.csv").groupby("codec")[["input_bytes", "compressed_bytes"]].mean()
    files = {}
    for f in RESULTS.glob("datasets/sf*.txt"):          # output of hdfs dfs -du: size, size with replicas, path
        for line in f.read_text().splitlines():
            size, replicas, path = line.split()
            codec = path.split("/")[-2]
            files[codec] = files.get(codec, 0) + int(size)
    shuffle_off = select(runs, sf=sf, storage="none", shuffle="off")
    ratios = {}
    for codec, bench_name in [("lz4", "lz4"), ("zstd", "zstd-1")]:
        shuffle_codec = select(runs, sf=sf, storage="none", shuffle=codec)
        ratios[codec] = [
            bench.loc[bench_name, "input_bytes"] / bench.loc[bench_name, "compressed_bytes"],
            files["none"] / files[codec],
            shuffle_off[shuffle_off["app"] != "cpu_bound"]["shuffle_write_bytes"].sum()
            / shuffle_codec[shuffle_codec["app"] != "cpu_bound"]["shuffle_write_bytes"].sum(),
        ]
    x = range(3)
    axes[0].bar([i - 0.2 for i in x], ratios["lz4"], 0.4, label="lz4", color=COLORS["lz4"])
    axes[0].bar([i + 0.2 for i in x], ratios["zstd"], 0.4, label="zstd", color=COLORS["zstd"])
    axes[0].set_xticks(list(x), ["benchmark", "files on HDFS", "shuffle data"])
    axes[0].set_ylabel("Compression ratio")
    axes[0].set_ylim(0, 4)
    axes[0].set_title("Compression ratio vs algorithm")
    axes[0].legend()

    # 2. execution time of C0-C3, relative to C0
    for i, config in enumerate(CONFIGS):
        storage, shuffle = CONFIGS[config]
        means, errors = [], []
        for app in APPS:
            base = select(runs, app, sf, "none", "off")["time_s"].mean()
            times = select(runs, app, sf, storage, shuffle)["time_s"]
            means.append(times.mean() / base * 100)
            errors.append(times.std() / base * 100)
        axes[1].bar([j + (i - 1.5) * 0.2 for j in range(3)], means, 0.2, yerr=errors, capsize=2, label=config,
                    color=["gray", "orangered", "royalblue", "purple"][i])
    axes[1].set_xticks(range(3), list(APPS.values()))
    axes[1].set_ylim(80, 109)
    axes[1].set_ylabel("Execution time (% of C0)")
    axes[1].set_title("Configurations C0-C3, SF 20")
    axes[1].legend(ncol=4)

    # 3. % faster than without compression, for each workload type
    cases = [("files lz4", "lz4", "off"), ("files zstd", "zstd", "off"), ("shuffle lz4", "none", "lz4"),
             ("shuffle zstd", "none", "zstd")]
    colors = ["lightsteelblue", "lightsalmon", "royalblue", "orangered"]
    for i, (label, storage, shuffle) in enumerate(cases):
        means, errors = [], []
        for app in APPS:
            mean, error = percent_faster(runs, app, sf, storage, shuffle)
            means.append(mean)
            errors.append(error)
        axes[2].bar([j + (i - 1.5) * 0.2 for j in range(3)], means, 0.2, yerr=errors, capsize=2, label=label,
                    color=colors[i])
    axes[2].axhline(0, color="black", linewidth=0.8)
    axes[2].set_xticks(range(3), list(APPS.values()))
    axes[2].set_ylabel("Faster than no compression (%)")
    axes[2].set_title("Execution time vs workload type, SF 20")
    axes[2].legend()
    save(fig, "fig4_ratio_configurations_workload")


def tables(runs):
    """Codec micro-benchmark, workload characterisation (no compression) and the means of every configuration."""
    TABLES.mkdir(parents=True, exist_ok=True)
    bench = pd.read_csv(RESULTS / "codec_bench.csv")
    bench["ratio"] = bench["input_bytes"] / bench["compressed_bytes"]
    bench["compress_mb_s"] = bench["input_bytes"] / bench["compress_s"] / 1e6
    bench["decompress_mb_s"] = bench["input_bytes"] / bench["decompress_s"] / 1e6
    columns = ["ratio", "compress_s", "decompress_s", "compress_mb_s", "decompress_mb_s"]
    bench.groupby("codec")[columns].median().round(3).to_csv(TABLES / "codec_bench.csv")

    c0 = select(runs, storage="none", shuffle="off").copy()
    c0["cpu_share"] = c0["executor_cpu_ns"] / 1e6 / c0["executor_run_ms"]   # CPU time / task time
    columns = ["time_s", "cpu_share", "cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "disk_gb", "net_gb",
               "shuffle_gb"]
    c0.groupby(["app", "sf"])[columns].mean().round(3).to_csv(TABLES / "workload_characterisation.csv")

    columns = ["time_s", "cpu_s", "cpu_busy_pct", "disk_gb", "disk_mb_s", "net_gb", "net_mb_s", "shuffle_gb",
               "shuffle_s"]
    runs.groupby(["app", "sf", "storage", "shuffle"])[columns].agg(["mean", "std"]).round(3) \
        .to_csv(TABLES / "summary.csv")
    print("   tables in", TABLES)


def main():
    runs = load_runs()
    figure_vs_size(runs, [("storage", "time_s", "Execution time (s)"), ("shuffle", "time_s", "Execution time (s)")],
                   "fig1_time_vs_size")
    figure_vs_size(runs, [("storage", "cpu_s", "CPU time (s)"), ("shuffle", "cpu_s", "CPU time (s)")],
                   "fig2_cpu_vs_size")
    figure_vs_size(runs, [("storage", "disk_gb", "Disk read + write (GB)"),
                          ("shuffle", "net_gb", "Network received (GB)")], "fig3_disk_network_vs_size")
    summary_figure(runs)
    tables(runs)


if __name__ == "__main__":
    main()
