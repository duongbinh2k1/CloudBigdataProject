"""Figures and tables of the report.

    .venv/bin/python analysis/report.py results/aws/aws

E1 = runs with shuffle off, E2 = runs with storage none, E3 = C0..C3 of `combined` in config.yaml.
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

HIGHLIGHT = ["lz4", "zstd"]          # main figures; all/ has every codec
APPS = ["io_bound", "balanced", "cpu_bound"]
APP_NAMES = {"io_bound": "I/O-bound", "balanced": "Balanced", "cpu_bound": "CPU-bound"}
# same color for a codec in every figure, gray = no compression
COLORS = {"none": "#8a8984", "off": "#8a8984", "lz4": "#2a78d6", "zstd": "#eb6834",
          "snappy": "#1baf7a", "gzip": "#eda100", "lzf": "#e87ba4"}
MARKERS = {"none": "o", "off": "o", "lz4": "s", "zstd": "^", "snappy": "D", "gzip": "v", "lzf": "P"}
CONFIG_COLORS = {"C0": "#8a8984", "C1": "#2a78d6", "C2": "#eb6834", "C3": "#1baf7a"}
METRICS = {   # column -> axis label
    "time_s": "Execution time (s)",
    "cpu_s": "CPU time, all executors (s)",
    "disk_gb": "Disk read + write, all nodes (GB)",
    "net_gb": "Network traffic, all nodes (GB)",
    "shuffle_write_gb": "Shuffle data written (GB)",
}

plt.rcParams.update({"axes.grid": True, "grid.color": "#e4e3df", "axes.axisbelow": True,
                     "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
                     "font.size": 8, "axes.titlesize": 10, "figure.dpi": 100})


def label(codec):
    return "No compression" if codec in ("none", "off") else codec.upper() if codec == "lz4" else codec.capitalize()


def load_runs(run_dir):
    runs = pd.read_csv(run_dir / "runs.csv")
    runs = runs[runs["status"] == "ok"].copy()
    runs["time_s"] = runs["duration_ms"] / 1000
    runs["cpu_s"] = runs["executor_cpu_ns"] / 1e9
    runs["disk_gb"] = (runs["disk_read_bytes"] + runs["disk_write_bytes"]) / 1e9
    runs["net_gb"] = runs["net_rx_bytes"] / 1e9            # bytes received by all nodes
    runs["shuffle_write_gb"] = runs["shuffle_write_bytes"] / 1e9
    return runs


def mean_std(runs, keys, metric):
    return runs.groupby(keys)[metric].agg(["mean", "std", "count"]).reset_index()


def storage_ratio(datasets_dir):
    # ratio = size without compression / size with the codec (from hdfs dfs -du)
    rows = []
    for f in sorted(datasets_dir.glob("sf*.txt")):
        sizes = {}
        for line in f.read_text().split("\n"):
            if line.strip():
                size, _, path = line.split()          # size, size with replicas, hdfs path
                codec = path.split("/")[-2]
                sizes[codec] = sizes.get(codec, 0) + int(size)
        for codec, size in sizes.items():
            if codec != "none":
                rows.append({"sf": f.stem[2:], "codec": codec, "ratio": sizes["none"] / size})
    return pd.DataFrame(rows, columns=["sf", "codec", "ratio"])


def shuffle_ratio(e2):
    # shuffle bytes without compression / with the codec, per app (mean over the sizes)
    per = e2.groupby(["app", "sf", "shuffle"])["shuffle_write_bytes"].mean().unstack("shuffle")
    ratio = per.rdiv(per["off"], axis=0).drop(columns="off")
    return ratio.groupby("app").mean().reset_index().melt(id_vars="app", var_name="codec", value_name="ratio")


def speedup(runs, level, baseline):
    # % faster than without compression (negative = slower)
    m = mean_std(runs, ["app", "sf", level], "time_s")
    base = m[m[level] == baseline][["app", "sf", "mean"]].rename(columns={"mean": "baseline_s"})
    m = m[m[level] != baseline].merge(base, on=["app", "sf"])
    m["speedup_pct"] = (m["baseline_s"] - m["mean"]) / m["baseline_s"] * 100
    return m.rename(columns={level: "codec", "mean": "time_s", "std": "time_std"})


def save(fig, out_dir, name):
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_dir / f"{name}.png", dpi=200, bbox_inches="tight")
    fig.savefig(out_dir / f"{name}.pdf", bbox_inches="tight")
    plt.close(fig)
    print("  ", out_dir / name)


def apps_in(df):
    return [a for a in APPS if a in set(df["app"])]


def metric_vs_size(runs, level, codecs, metric, title):
    # one panel per app, one line per codec, error bars = std over the repetitions
    m = mean_std(runs, ["app", "sf", level], metric)
    apps = apps_in(m)
    fig, axes = plt.subplots(1, len(apps), figsize=(3.6 * len(apps), 3.2), squeeze=False)
    for ax, app in zip(axes[0], apps):
        for codec in codecs:
            r = m[(m["app"] == app) & (m[level] == codec)].sort_values("sf")
            if not r.empty:
                ax.errorbar(r["sf"], r["mean"], yerr=r["std"].fillna(0), label=label(codec), color=COLORS[codec],
                            marker=MARKERS[codec], linestyle="--" if codec in ("none", "off") else "-", capsize=3)
        ax.set_title(APP_NAMES[app])
        ax.set_xlabel("Dataset size (SF ≈ GB)")
        ax.set_ylabel(METRICS[metric])
        ax.set_xticks(sorted(m["sf"].unique()))
        ax.set_ylim(0, ax.get_ylim()[1] * 1.1)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(labels))
    fig.suptitle(title)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    return fig


def grouped_bars(ax, df, group_col, groups, series, value_col, err_col=None):
    width = 0.8 / len(series)
    for i, codec in enumerate(series):
        r = df[df["codec"] == codec].set_index(group_col).reindex(groups)
        x = np.arange(len(groups)) + (i - (len(series) - 1) / 2) * width
        yerr = r[err_col].fillna(0) if err_col else None
        ax.bar(x, r[value_col], width, yerr=yerr, capsize=3, label=label(codec), color=COLORS[codec])
    ax.set_xticks(np.arange(len(groups)), [APP_NAMES.get(g, g) for g in groups])


def compression_ratio_fig(st_ratio, sh_ratio, codecs):
    fig, (left, right) = plt.subplots(1, 2, figsize=(8, 3.2))
    st = st_ratio[st_ratio["codec"].isin(codecs)].groupby("codec")["ratio"].mean().reindex(codecs).dropna()
    left.bar([label(c) for c in st.index], st, color=[COLORS[c] for c in st.index])
    for i, v in enumerate(st):
        left.text(i, v, f"{v:.2f}×", ha="center", va="bottom")
    left.set_title("Storage (text files on HDFS)")
    left.set_ylabel("Compression ratio (uncompressed / compressed)")
    shuffle_codecs = [c for c in codecs if c in set(sh_ratio["codec"])]
    grouped_bars(right, sh_ratio, "app", apps_in(sh_ratio), shuffle_codecs, "ratio")
    right.set_title("Shuffle (data written by map tasks)")
    right.set_ylabel("Compression ratio (off / compressed)")
    right.legend()
    fig.tight_layout()
    return fig


def configurations_fig(runs, combined):
    x, y = combined["storage"], combined["shuffle"]
    configs = {("none", "off"): "C0", (x, "off"): "C1", ("none", y): "C2", (x, y): "C3"}
    runs = runs.assign(config=[configs.get(k) for k in zip(runs["storage"], runs["shuffle"])]).dropna(subset=["config"])
    sf = runs["sf"].max()
    m = mean_std(runs[runs["sf"] == sf], ["app", "config"], "time_s")
    apps = apps_in(m)
    fig, axes = plt.subplots(1, len(apps), figsize=(3.6 * len(apps), 3.2), squeeze=False)
    names = ["C0", "C1", "C2", "C3"]
    for ax, app in zip(axes[0], apps):
        r = m[m["app"] == app].set_index("config").reindex(names)
        ax.bar(names, r["mean"], yerr=r["std"].fillna(0), capsize=3, color=[CONFIG_COLORS[c] for c in names])
        ax.set_title(APP_NAMES[app])
        ax.set_ylabel(METRICS["time_s"])
    legend = ["C0: none / off", f"C1: {x} / off", f"C2: none / {y}", f"C3: {x} / {y}"]
    fig.legend([plt.Rectangle((0, 0), 1, 1, color=CONFIG_COLORS[c]) for c in names], legend,
               loc="lower center", ncol=4, title="storage / shuffle")
    fig.suptitle(f"Combined compression, SF {sf:g}")
    fig.tight_layout(rect=(0, 0.12, 1, 1))
    return fig


def workload_fig(sp_storage, sp_shuffle, codecs):
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.4))
    for ax, sp, title in ((axes[0], sp_storage, "Storage"), (axes[1], sp_shuffle, "Shuffle")):
        sf = sp["sf"].max()
        r = sp[sp["sf"] == sf]
        grouped_bars(ax, r, "app", apps_in(r), [c for c in codecs if c in set(r["codec"])], "speedup_pct")
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(f"{title} compression, SF {sf:g}")
        ax.set_ylabel("Faster than no compression (%)")
        ax.legend()
    fig.tight_layout()
    return fig


def tradeoff_fig(raw):
    raw = raw[raw["input"] == "csv"].copy()
    raw["ratio"] = raw["input_bytes"] / raw["compressed_bytes"]
    raw["comp_mb_s"] = raw["input_bytes"] / raw["comp_wall_ns"] * 1e3
    raw["decomp_mb_s"] = raw["input_bytes"] / raw["decomp_wall_ns"] * 1e3
    summary = raw.groupby("codec")[["ratio", "comp_mb_s", "decomp_mb_s"]].median().reset_index()
    fig, ax = plt.subplots(figsize=(5, 3.6))
    for r in summary.itertuples():
        family = r.codec.split("-")[0]
        ax.scatter(r.comp_mb_s, r.ratio, s=50, color=COLORS[family], marker=MARKERS[family])
        ax.annotate(r.codec, (r.comp_mb_s, r.ratio), xytext=(6, 0), textcoords="offset points", va="center")
    ax.set_xscale("log")
    ax.margins(x=0.15)
    ax.set_xlabel("Compression speed (MB/s, log scale)")
    ax.set_ylabel("Compression ratio")
    ax.set_title("Codec micro-benchmark (one node, lineitem text)")
    return fig, summary


def main(run_dir):
    run_dir = Path(run_dir)
    env_dir = run_dir.parent
    out = Path(__file__).parent / "figures" / env_dir.name / run_dir.name
    tables = out / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    config = yaml.safe_load((run_dir / "config.yaml").read_text())
    runs = load_runs(run_dir)
    e1 = runs[runs["shuffle"] == "off"]
    e2 = runs[runs["storage"] == "none"]

    st_ratio = storage_ratio(env_dir / "datasets")
    sh_ratio = shuffle_ratio(e2)
    sp_storage = speedup(e1, "storage", "none")
    sp_shuffle = speedup(e2, "shuffle", "off")

    all_storage = sorted(set(e1["storage"]) - {"none"})
    all_shuffle = sorted(set(e2["shuffle"]) - {"off"})
    for directory, storage_codecs, shuffle_codecs in (
            (out, [c for c in HIGHLIGHT if c in all_storage], [c for c in HIGHLIGHT if c in all_shuffle]),
            (out / "all", all_storage, all_shuffle)):
        for metric, n in (("time_s", 1), ("cpu_s", 2), ("disk_gb", 3), ("net_gb", 4)):
            save(metric_vs_size(e1, "storage", ["none"] + storage_codecs, metric, "Storage compression (E1)"),
                 directory, f"fig{n}_{metric}_storage")
            save(metric_vs_size(e2, "shuffle", ["off"] + shuffle_codecs, metric, "Shuffle compression (E2)"),
                 directory, f"fig{n}_{metric}_shuffle")
        codecs = sorted(set(storage_codecs) | set(shuffle_codecs))
        save(compression_ratio_fig(st_ratio, sh_ratio, codecs), directory, "fig5_compression_ratio")
        save(workload_fig(sp_storage, sp_shuffle, codecs), directory, "fig7_workload")

    if config.get("combined"):
        save(configurations_fig(runs, config["combined"]), out, "fig6_configurations")

    bench = env_dir / "codec_bench" / "raw.csv"
    if bench.exists():
        fig, summary = tradeoff_fig(pd.read_csv(bench))
        save(fig, out, "fig5b_codec_microbench")
        summary.round(2).to_csv(tables / "codec_microbench.csv", index=False)

    # profile of each app without compression (C0)
    c0 = runs[(runs["storage"] == "none") & (runs["shuffle"] == "off")].copy()
    c0["cpu_ratio"] = c0["executor_cpu_ns"] / 1e6 / c0["executor_run_ms"]
    moved = c0["input_bytes"] + c0["shuffle_read_bytes"] + c0["shuffle_write_bytes"] + c0["output_bytes"]
    c0["mb_per_cpu_s"] = moved / 1e6 / c0["cpu_s"]
    char = c0.groupby(["app", "sf"])[["time_s", "cpu_ratio", "mb_per_cpu_s", "cpu_busy_pct", "cpu_iowait_pct",
                                      "shuffle_write_gb"]].mean().reset_index()

    char.round(3).to_csv(tables / "workload_characterisation.csv", index=False)
    st_ratio.round(3).to_csv(tables / "ratio_storage.csv", index=False)
    sh_ratio.round(3).to_csv(tables / "ratio_shuffle.csv", index=False)
    pd.concat([sp_storage.assign(level="storage"), sp_shuffle.assign(level="shuffle")]).round(2) \
        .to_csv(tables / "speedup.csv", index=False)
    summary = [mean_std(runs, ["app", "sf", "storage", "shuffle"], m).assign(metric=m) for m in METRICS]
    pd.concat(summary).round(3).to_csv(tables / "summary.csv", index=False)
    print("tables in", tables)


if __name__ == "__main__":
    main(sys.argv[1])
