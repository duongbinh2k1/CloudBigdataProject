"""Figures (analysis/figures/) and tables (analysis/tables/) from results/. Run with: make analysis

Every point or bar = mean of the repetitions; error bars = standard deviation.
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
COLORS = {"none": "gray", "off": "gray", "gzip": "goldenrod", "snappy": "seagreen", "lz4": "royalblue",
          "zstd": "orangered", "zstd-1": "orangered", "zstd-3": "darkred"}
plt.rcParams.update({"axes.grid": True, "font.size": 8, "axes.titlesize": 9, "legend.fontsize": 7})


def load_runs():
    runs = pd.read_csv(RESULTS / "runs.csv")
    runs = runs[runs["status"] == "ok"].copy()
    runs["cpu_s"] = runs["executor_cpu_ns"] / 1e9
    runs["disk_gb"] = (runs["disk_read_bytes"] + runs["disk_write_bytes"]) / 1e9
    runs["net_gb"] = runs["net_rx_bytes"] / 1e9
    runs["shuffle_s"] = runs["shuffle_write_ns"] / 1e9 + runs["shuffle_fetch_wait_ms"] / 1e3
    runs["disk_mb_s"] = runs["disk_gb"] * 1000 / runs["time_s"]      # throughput, all nodes
    runs["net_mb_s"] = runs["net_gb"] * 1000 / runs["time_s"]
    return runs


def stats(runs, keys, metric):
    return runs.groupby(keys)[metric].agg(["mean", "std"]).fillna(0).reset_index()


def save(fig, name):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES / f"{name}.png", dpi=150)
    plt.close(fig)
    print("  ", FIGURES / f"{name}.png")


def vs_size(runs, metric, ylabel, name):
    """Figures 1-4: metric vs dataset size, one line per codec. Row 1 = E1 (storage), row 2 = E2 (shuffle)."""
    fig, axes = plt.subplots(2, 3, figsize=(10, 5.5))
    for row, (level, runs_of_level) in enumerate([("storage", runs[runs["shuffle"] == "off"]),
                                                  ("shuffle", runs[runs["storage"] == "none"])]):
        s = stats(runs_of_level, ["app", "sf", level], metric)
        for ax, (app, title) in zip(axes[row], APPS.items()):
            for codec, r in s[s["app"] == app].groupby(level):
                ax.errorbar(r["sf"], r["mean"], yerr=r["std"], label=codec, color=COLORS[codec], marker="o", capsize=3)
            ax.set_title(f"{title} - {level} compression")
            ax.set_xlabel("Dataset size (SF, about GB)")
            ax.set_ylabel(ylabel)
            ax.set_ylim(bottom=0)
        axes[row][0].legend()
    save(fig, name)


def compression_ratio(runs):
    """Figure 5: compression ratio (size without compression / size with the codec) of every codec."""
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.2))
    bench = pd.read_csv(RESULTS / "codec_bench.csv")
    bench = bench[bench["input"] == "lineitem"].groupby("codec")[["input_bytes", "compressed_bytes"]].mean()
    ratio = bench["input_bytes"] / bench["compressed_bytes"]
    axes[0].bar(ratio.index, ratio, color=[COLORS[c] for c in ratio.index])
    axes[0].set_title("Micro-benchmark (one 75 MB file)")

    sizes = []        # results/datasets/sf*.txt: output of hdfs dfs -du (size, size with replicas, path)
    for f in RESULTS.glob("datasets/sf*.txt"):
        for line in f.read_text().splitlines():
            size, _, path = line.split()
            sizes.append({"sf": f.stem, "codec": path.split("/")[-2], "bytes": int(size)})
    per_codec = pd.DataFrame(sizes).groupby(["sf", "codec"])["bytes"].sum().unstack()
    ratio = (per_codec.rdiv(per_codec["none"], axis=0)).mean().drop("none")
    axes[1].bar(ratio.index, ratio, color=[COLORS[c] for c in ratio.index])
    axes[1].set_title("Storage: dataset on HDFS")

    e2 = runs[runs["storage"] == "none"].groupby(["app", "shuffle"])["shuffle_write_bytes"].mean().unstack()
    ratio = e2.rdiv(e2["off"], axis=0).drop(columns="off")
    ratio.index = [APPS[a] for a in ratio.index]
    ratio.plot.bar(ax=axes[2], color=[COLORS[c] for c in ratio.columns], rot=0)
    axes[2].set_title("Shuffle: data written by the map tasks")
    for ax in axes:
        ax.set_ylabel("Compression ratio")
    save(fig, "fig5_compression_ratio")


def codec_tradeoff():
    """Figure 8 + table: ratio vs speed of each codec (micro-benchmark), on the lineitem text and on random bytes."""
    bench = pd.read_csv(RESULTS / "codec_bench.csv")
    bench["ratio"] = bench["input_bytes"] / bench["compressed_bytes"]
    bench["compress_mb_s"] = bench["input_bytes"] / bench["compress_s"] / 1e6
    bench["decompress_mb_s"] = bench["input_bytes"] / bench["decompress_s"] / 1e6
    table = bench.groupby(["input", "codec"])[["ratio", "compress_mb_s", "decompress_mb_s"]].median().round(2)
    table.to_csv(TABLES / "codec_bench.csv")
    fig, ax = plt.subplots(figsize=(5, 3.5))
    for codec, r in table.loc["lineitem"].iterrows():
        ax.scatter(r["compress_mb_s"], r["ratio"], color=COLORS[codec], s=40)
        ax.annotate(codec, (r["compress_mb_s"], r["ratio"]), xytext=(5, 0), textcoords="offset points")
    ax.set_xscale("log")
    ax.set_xlabel("Compression speed, one thread (MB/s, log scale)")
    ax.set_ylabel("Compression ratio")
    ax.set_title("Codec trade-off: ratio vs CPU cost")
    save(fig, "fig8_codec_tradeoff")


def configurations(runs, combined):
    """Figure 6: execution time of C0-C3 for each app, at the largest size."""
    x, y = combined["storage"], combined["shuffle"]
    names = {("none", "off"): "C0", (x, "off"): "C1", ("none", y): "C2", (x, y): "C3"}
    runs = runs.assign(config=[names.get(k) for k in zip(runs["storage"], runs["shuffle"])]).dropna(subset=["config"])
    sf = runs["sf"].max()
    s = stats(runs[runs["sf"] == sf], ["app", "config"], "time_s")
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.2))
    for ax, (app, title) in zip(axes, APPS.items()):
        r = s[s["app"] == app].sort_values("config")
        ax.bar(r["config"], r["mean"], yerr=r["std"], capsize=3, color=["gray", "orangered", "royalblue", "purple"])
        ax.set_title(title)
        ax.set_ylabel("Execution time (s)")
    fig.suptitle(f"SF {sf}: C0 none/off, C1 {x}/off, C2 none/{y}, C3 {x}/{y}  (storage/shuffle)")
    save(fig, "fig6_configurations")


def workload(runs):
    """Figure 7: % faster than without compression, for each app (workload type), at the largest size."""
    sf = runs["sf"].max()
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.4))
    for ax, level, baseline, other, other_off in ((axes[0], "storage", "none", "shuffle", "off"),
                                                  (axes[1], "shuffle", "off", "storage", "none")):
        times = runs[(runs["sf"] == sf) & (runs[other] == other_off)].groupby(["app", level])["time_s"]
        mean, std = times.mean().unstack(), times.std().unstack()
        gain = mean.rsub(mean[baseline], axis=0).div(mean[baseline], axis=0).drop(columns=baseline) * 100
        error = std.div(mean[baseline], axis=0).drop(columns=baseline) * 100      # std of the codec's runs
        gain.index = error.index = [APPS[a] for a in gain.index]
        gain.plot.bar(ax=ax, yerr=error, capsize=3, color=[COLORS[c] for c in gain.columns], rot=0)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(f"{level.capitalize()} compression, SF {sf}")
        ax.set_ylabel("Faster than no compression (%)")
    save(fig, "fig7_workload_type")


def tables(runs):
    """Workload characterisation (§3: CPU, disk, network without compression) and all the means."""
    c0 = runs[(runs["storage"] == "none") & (runs["shuffle"] == "off")].copy()
    c0["cpu_share"] = c0["executor_cpu_ns"] / 1e6 / c0["executor_run_ms"]    # CPU time / task time
    moved = c0["input_bytes"] + c0["shuffle_write_bytes"] + c0["shuffle_read_bytes"] + c0["output_bytes"]
    c0["mb_moved_per_cpu_s"] = moved / 1e6 / c0["cpu_s"]
    columns = ["time_s", "cpu_share", "mb_moved_per_cpu_s", "cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct",
               "disk_gb", "net_gb", "shuffle_write_bytes"]
    c0.groupby(["app", "sf"])[columns].mean().round(3).to_csv(TABLES / "workload_characterisation.csv")
    metrics = ["time_s", "cpu_s", "disk_gb", "disk_mb_s", "net_gb", "net_mb_s", "shuffle_write_bytes", "shuffle_s",
               "spill_bytes"]
    runs.groupby(["app", "sf", "storage", "shuffle"])[metrics].agg(["mean", "std"]).round(3) \
        .to_csv(TABLES / "summary.csv")
    print("   tables in", TABLES)


def main():
    TABLES.mkdir(parents=True, exist_ok=True)
    runs = load_runs()
    vs_size(runs, "time_s", "Execution time (s)", "fig1_time_vs_size")
    vs_size(runs, "cpu_s", "CPU time of the executors (s)", "fig2_cpu_vs_size")
    vs_size(runs, "disk_gb", "Disk read + write, all nodes (GB)", "fig3_disk_vs_size")
    vs_size(runs, "net_gb", "Network received, all nodes (GB)", "fig4_network_vs_size")
    compression_ratio(runs)
    configurations(runs, yaml.safe_load(Path("bench/experiments.yaml").read_text())["combined"])
    workload(runs)
    codec_tradeoff()
    tables(runs)


if __name__ == "__main__":
    main()
