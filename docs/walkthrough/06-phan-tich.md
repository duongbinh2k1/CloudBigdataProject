# 06 — Phân tích: từ `runs.csv` đến đồ thị và bảng

`analysis/report.py` chạy **trên laptop** (`make analysis` hoặc `make analysis ENV=aws`). Đọc `results/<env>/`, ghi đồ thị và bảng vào `analysis/figures/<env>/`. Dùng **pandas** (bảng dữ liệu) và **matplotlib** (vẽ), cài trong `.venv` (`analysis/requirements.txt`).

## 1. Đầu vào và đầu ra

| Đọc | Dùng cho |
|---|---|
| `results/<env>/runs.csv` | mọi đồ thị về thời gian, CPU, đĩa, mạng, shuffle |
| `results/<env>/config.yaml` | biết cặp codec của E3 (`combined`) |
| `results/<env>/datasets/sf*.txt` | tỉ lệ nén lưu trữ |
| `results/<env>/codec_bench/raw.csv` | micro-benchmark |

```
analysis/figures/<env>/
├── fig1_time_s_storage.png/.pdf     fig1_time_s_shuffle     thời gian theo kích thước
├── fig2_cpu_s_storage               fig2_cpu_s_shuffle      CPU theo kích thước
├── fig3_disk_gb_storage             fig3_disk_gb_shuffle    I/O đĩa theo kích thước
├── fig4_net_gb_storage              fig4_net_gb_shuffle     mạng theo kích thước
├── fig5_compression_ratio           tỉ lệ nén (lưu trữ + shuffle)
├── fig5b_codec_microbench           micro-benchmark: tỉ lệ theo tốc độ
├── fig6_configurations              C0–C3
├── fig7_workload                    nhanh/chậm hơn bao nhiêu % theo kiểu app
├── all/                             như trên nhưng đủ 4 codec (thư mục chính chỉ vẽ LZ4 + Zstd)
└── tables/                          các bảng CSV
```

## 2. Đọc và chuẩn bị dữ liệu

```python
HIGHLIGHT = ["lz4", "zstd"]       # đồ thị chính: không nén + LZ4 (nhanh) + Zstd (nén mạnh); all/ có đủ
COLORS = {"none": "#8a8984", "off": "#8a8984", "lz4": "#2a78d6", "zstd": "#eb6834", …}   # mỗi codec một màu cố định
METRICS = {"time_s": "Execution time (s)", "cpu_s": …, "disk_gb": …, "net_gb": …, "shuffle_write_gb": …}
```

```python
def load_runs(run_dir):
    runs = pd.read_csv(run_dir / "runs.csv")                 # đọc CSV thành bảng
    runs = runs[runs["status"] == "ok"].copy()               # bỏ các lần lỗi
    runs["time_s"] = runs["duration_ms"] / 1000              # ms → giây
    runs["cpu_s"] = runs["executor_cpu_ns"] / 1e9            # ns → giây
    runs["disk_gb"] = (runs["disk_read_bytes"] + runs["disk_write_bytes"]) / 1e9   # đọc + ghi, mọi máy
    runs["net_gb"] = runs["net_rx_bytes"] / 1e9              # byte NHẬN, mọi máy
    runs["shuffle_write_gb"] = runs["shuffle_write_bytes"] / 1e9
    return runs
```
Mạng chỉ lấy chiều **nhận**: mỗi byte gửi từ máy này sang máy kia được tính một lần ở máy nhận. Cộng cả gửi lẫn nhận thì đếm hai lần.

```python
def mean_std(runs, keys, metric):
    return runs.groupby(keys)[metric].agg(["mean", "std", "count"]).reset_index()
```
Gom các lần lặp **cùng cấu hình** (ví dụ cùng app, cùng SF, cùng codec), tính **trung bình**, **độ lệch chuẩn** (độ dao động giữa các lần lặp) và số lần. Độ lệch chuẩn là chiều cao **error bar** trên đồ thị.

Trong `main()`:
```python
e1 = runs[runs["shuffle"] == "off"]      # E1: mọi lần có shuffle tắt nén → so sánh các codec LƯU TRỮ
e2 = runs[runs["storage"] == "none"]     # E2: mọi lần không nén lưu trữ → so sánh các codec SHUFFLE
```
Không cần cột "đây là E1 hay E2": suy ra từ cấu hình. Lần `(none, off)` thuộc cả E1 và E2: nó là mốc so sánh của cả hai.

## 3. Các phép tính

**Tỉ lệ nén lưu trữ**
```python
def storage_ratio(datasets_dir):
    for f in sorted(datasets_dir.glob("sf*.txt")):          # mỗi kích thước
        for line in f.read_text().split("\n"):              # "228365901  685097703  hdfs://…/sf1/gzip/lineitem"
            size, _, path = line.split()                    # kích thước 1 bản, (bỏ: kể cả bản sao), đường dẫn
            codec = path.split("/")[-2]                     # "gzip"
            sizes[codec] = sizes.get(codec, 0) + int(size)  # cộng orders + lineitem
        ratio = sizes["none"] / sizes[codec]                # ví dụ 933 MB / 283 MB = 3,30
```

**Tỉ lệ nén shuffle**
```python
def shuffle_ratio(e2):
    per = e2.groupby(["app", "sf", "shuffle"])["shuffle_write_bytes"].mean().unstack("shuffle")
    ratio = per.rdiv(per["off"], axis=0).drop(columns="off")      # byte khi tắt nén / byte khi có codec
    return ratio.groupby("app").mean()…                           # trung bình qua các kích thước
```
`unstack("shuffle")` biến mỗi codec shuffle thành một cột; `rdiv(per["off"])` = cột `off` chia cho từng cột.

**Speedup (% nhanh hơn so với không nén)**
```python
def speedup(runs, level, baseline):         # level = "storage" (E1) hoặc "shuffle" (E2); baseline = "none"/"off"
    m = mean_std(runs, ["app", "sf", level], "time_s")
    base = m[m[level] == baseline][["app", "sf", "mean"]]          # thời gian không nén, mỗi (app, SF)
    m = m[m[level] != baseline].merge(base, on=["app", "sf"])      # ghép với thời gian có nén
    m["speedup_pct"] = (m["baseline_s"] - m["mean"]) / m["baseline_s"] * 100
```
Ví dụ không nén 43,3 s, có LZ4 35,8 s → (43,3 − 35,8) / 43,3 × 100 = **+17 %** (nhanh hơn). Số âm = chậm hơn.

**Đặc tính workload** (chứng minh mỗi app đúng kiểu, §3 của đề)
```python
c0 = runs[(runs["storage"] == "none") & (runs["shuffle"] == "off")]     # chỉ các lần không nén
c0["cpu_ratio"] = c0["executor_cpu_ns"] / 1e6 / c0["executor_run_ms"]   # CPU / thời gian task chạy
moved = c0["input_bytes"] + c0["shuffle_read_bytes"] + c0["shuffle_write_bytes"] + c0["output_bytes"]
c0["mb_per_cpu_s"] = moved / 1e6 / c0["cpu_s"]                          # MB di chuyển / giây CPU
```
- `cpu_ratio` gần 1 → task gần như lúc nào cũng dùng CPU (CPU-bound); thấp → task hay phải chờ đĩa/mạng.
- `mb_per_cpu_s` cao → di chuyển nhiều dữ liệu so với lượng tính toán (I/O-bound); thấp → tính nhiều (CPU-bound).
- Kèm `cpu_busy_pct`, `cpu_iowait_pct` từ `sar`.

## 4. Các đồ thị

**fig1–fig4: số đo theo kích thước** (`metric_vs_size`)
```python
m = mean_std(runs, ["app", "sf", level], metric)
fig, axes = plt.subplots(1, len(apps), …)                  # một ô cho mỗi app
for ax, app in zip(axes[0], apps):
    for codec in codecs:                                    # một đường cho mỗi codec
        r = m[(m["app"] == app) & (m[level] == codec)].sort_values("sf")
        ax.errorbar(r["sf"], r["mean"], yerr=r["std"].fillna(0), …,
                    linestyle="--" if codec in ("none", "off") else "-")    # không nén: nét đứt
```
- Trục x: SF; trục y: số đo; error bar: độ lệch chuẩn (`fillna(0)`: chỉ 1 lần lặp thì không có error bar).
- Mỗi số đo vẽ hai lần: `_storage` (E1, các codec lưu trữ) và `_shuffle` (E2, các codec shuffle).

| Đồ thị | Số đo | Trả lời |
|---|---|---|
| fig1 | `time_s` (thời gian app) | nén nhanh hay chậm hơn, theo kích thước |
| fig2 | `cpu_s` (CPU executor) | chi phí CPU của nén/giải nén |
| fig3 | `disk_gb` (sar) | nén lưu trữ giảm I/O đĩa bao nhiêu |
| fig4 | `net_gb` (sar) | nén shuffle giảm lưu lượng mạng bao nhiêu |

**fig5: tỉ lệ nén** (`compression_ratio_fig`): trái là cột tỉ lệ nén lưu trữ của từng codec; phải là tỉ lệ nén shuffle theo từng app.

**fig5b: micro-benchmark** (`tradeoff_fig`)
```python
raw = raw[raw["input"] == "csv"]                                  # chỉ dữ liệu lineitem thật
raw["ratio"] = raw["input_bytes"] / raw["compressed_bytes"]
raw["comp_mb_s"] = raw["input_bytes"] / raw["comp_wall_ns"] * 1e3  # byte/ns × 1000 = MB/s
summary = raw.groupby("codec")[["ratio", "comp_mb_s", "decomp_mb_s"]].median()   # trung vị 5 lần
ax.scatter(comp_mb_s, ratio) ; ax.set_xscale("log")              # mỗi codec một điểm; trục x log
```
Góc **trên phải** là tốt nhất (nén mạnh và nhanh). Trục x dạng log vì tốc độ chênh hơn 10 lần (gzip ~27, lz4 ~360 MB/s).

**fig6: C0–C3** (`configurations_fig`): mỗi app một ô, 4 cột thời gian (C0 không nén, C1 chỉ lưu trữ, C2 chỉ shuffle, C3 cả hai), ở **kích thước lớn nhất**.

**fig7: theo kiểu workload** (`workload_fig`): speedup (%) của từng codec cho từng app, ở kích thước lớn nhất; trái: nén lưu trữ, phải: nén shuffle. Đường ngang ở 0: trên là nhanh hơn, dưới là chậm hơn.

## 5. Các bảng (`tables/`)

| Bảng | Nội dung |
|---|---|
| `summary.csv` | trung bình, độ lệch chuẩn, số lần của mọi số đo, cho mọi (app, SF, codec lưu trữ, codec shuffle); thêm `shuffle_fetch_wait_ms` |
| `speedup.csv` | speedup (%) của mọi codec, cả hai mức |
| `ratio_storage.csv`, `ratio_shuffle.csv` | tỉ lệ nén |
| `codec_microbench.csv` | micro-benchmark: tỉ lệ, MB/s nén, MB/s giải nén |
| `workload_characterisation.csv` | `cpu_ratio`, MB/giây CPU, %CPU, %iowait, shuffle của mỗi app khi không nén |

## 6. Bảy câu hỏi của đề và chỗ lấy số

| Câu hỏi | Xem |
|---|---|
| 1. Codec nào cân bằng tốt nhất giữa tỉ lệ nén và CPU? | `fig5b`, `fig5`, `fig2` |
| 2. Nén có luôn giúp app I/O-bound? | `fig1` ô I/O-bound, `speedup.csv` (có codec nào âm?) |
| 3. Ảnh hưởng tới app CPU-bound? | `fig1`/`fig2` ô CPU-bound |
| 4. Lợi ích thay đổi thế nào khi dữ liệu lớn lên? | độ dốc các đường trong `fig1`, `speedup.csv` theo SF |
| 5. Nén có lợi hơn cho lưu trữ hay mạng? | hai nửa của `fig7`; `fig3` với `fig4`; `shuffle_fetch_wait_ms` |
| 6. Kết hợp hai mức có làm chậm đi không? | `fig6`: C3 so với C1, C2 |
| 7. Khi nào nên tắt nén? | các speedup âm; micro-benchmark trên dữ liệu ngẫu nhiên; app CPU-bound |
