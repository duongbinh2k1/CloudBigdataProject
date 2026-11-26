# 05 — Đo lường: chạy thí nghiệm và lấy số đo

Các file trong `bench/`:

| File | Vai trò |
|---|---|
| `run_all.sh` | cả quy trình trên master: build → dữ liệu → micro-benchmark → thí nghiệm |
| `experiments-dev.yaml`, `experiments-aws.yaml` | **thiết kế thí nghiệm**: kích thước, số lần lặp, codec, app |
| `run_experiments.py` | chạy **từng lần đo**, ghi `runs.csv` |
| `metrics.py` | lấy số đo từ **History Server** và **sar** |
| `CodecBench.java` + `codec_bench.sh` | **micro-benchmark** từng codec trên một máy |

## 1. File thiết kế thí nghiệm (YAML)

`bench/experiments-aws.yaml`:
```yaml
seed: 20261111                                     # seed xáo trộn thứ tự chạy (cố định → tái lập được)
repetitions: 1                                     # số lần lặp mỗi cấu hình (TẠM: chính thức 3 hoặc 5)
sizes: [1]                                         # các SF (TẠM: chính thức [1, 5, 10, 20])
apps: [io_bound, cpu_bound, balanced]
storage_codecs: [none, gzip, snappy, lz4, zstd]    # E1: các codec lưu trữ
shuffle_codecs: ["off", lz4, snappy, zstd]         # E2: các codec shuffle
combined: {storage: zstd, shuffle: lz4}            # E3: cặp codec cho C1, C2, C3
rounds: {cpu_bound: 64, balanced: 8}               # số vòng SHA-256 của từng app
```
- `"off"` phải để trong ngoặc kép: YAML hiểu `off` (không ngoặc) là giá trị đúng/sai `false`.
- `experiments-dev.yaml` giống vậy nhưng `sizes: [0.1]`, 1 lần lặp, không có zstd lưu trữ (Mac không có thư viện native), cặp E3 là gzip/lz4.

## 2. `bench/run_all.sh`: cả quy trình

Chạy trên master: `bash bench/run_all.sh dev` (từ `make run`) hoặc `bash bench/run_all.sh aws` (từ `make run ENV=aws`, trong tmux).

```bash
set -e
env=$1                                   # dev hoặc aws
config=bench/experiments-$env.yaml
results=results/$env
cd /opt/work
```

```bash
yaml() {
  python3 -c "import yaml; print(' '.join(str(v) for v in yaml.safe_load(open('$config'))['$1']))"
}
sizes=$(yaml sizes)                              # ví dụ "1 5 10 20"
codecs=$(yaml storage_codecs | tr ' ' ',')       # "none gzip snappy lz4 zstd" → "none,gzip,snappy,lz4,zstd"
```
Bash không đọc được YAML, nên gọi Python một dòng để lấy một danh sách trong YAML, in ra cách nhau bằng dấu cách.

```bash
bash java/build.sh                               # 1. build jar
```

```bash
mkdir -p $results/datasets
for sf in $sizes; do                             # 2. dữ liệu cho từng kích thước
  if [ -f $results/datasets/sf$sf.txt ]; then
    echo "dataset SF $sf already generated"; continue
  fi
  spark-submit --class compression.datagen.Generator java/build/cloud-bigdata-project.jar $sf hdfs://master:9000/data $codecs
  hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > $results/datasets/sf$sf.txt
done
```
- Sinh xong một kích thước thì mới ghi `sf<N>.txt`; có file đó nghĩa là đã xong → lần sau bỏ qua. Bị ngắt giữa chừng thì kích thước đó được sinh lại từ đầu.
- `hdfs dfs -du -s "…/sf1/*/*"`: dung lượng của từng thư mục `sf1/<codec>/<bảng>`. Ví dụ một dòng: `228365901  685097703  hdfs://master:9000/data/sf1/gzip/lineitem` = 228 MB một bản, 685 MB tính cả 3 bản sao.

```bash
if [ ! -f $results/codec_bench/raw.csv ]; then       # 3. micro-benchmark (một lần)
  first=$(echo $sizes | cut -d' ' -f1)               # kích thước đầu tiên
  bash bench/codec_bench.sh hdfs://master:9000/data/sf$first/none/lineitem/part-00000 $results/codec_bench
fi
python3 bench/run_experiments.py $config $results    # 4. các lần đo
```

## 3. `bench/run_experiments.py`: các lần đo

### 3.1 Hằng số

```python
JAR = "java/build/cloud-bigdata-project.jar"
DATA = "hdfs://master:9000/data"          # dữ liệu do Generator ghi
OUTPUT = "hdfs://master:9000/results"     # output tạm của các app
HISTORY = "http://master:18080"           # History Server
WORKERS_FILE = "/opt/spark/conf/workers"  # danh sách worker (start-workers.sh cũng dùng)
CLASSES = {"io_bound": "compression.apps.IoBound", "cpu_bound": "compression.apps.CpuBound",
           "balanced": "compression.apps.Balanced"}
COLUMNS = (["run_id", "status", "app", "sf", "storage", "shuffle", "rep", "wall_s", "output_hdfs_bytes"]
           + metrics.SPARK_COLUMNS + metrics.SAR_COLUMNS)      # các cột của runs.csv
```

### 3.2 Danh sách lần chạy

```python
def configurations(cfg):
    pairs = {(s, "off") for s in cfg["storage_codecs"]}            # E1: (none,off) (gzip,off) (snappy,off) (lz4,off) (zstd,off)
    pairs |= {("none", s) for s in cfg["shuffle_codecs"]}          # E2: (none,off) (none,lz4) (none,snappy) (none,zstd)
    if cfg.get("combined"):                                        # E3: C0 (none,off) C1 (zstd,off) C2 (none,lz4) C3 (zstd,lz4)
        x, y = cfg["combined"]["storage"], cfg["combined"]["shuffle"]
        pairs |= {("none", "off"), (x, "off"), ("none", y), (x, y)}
    return pairs
```
`{…}` là **tập hợp** (set): cặp trùng chỉ giữ một. `(none, off)` có ở cả E1, E2, E3 nhưng chỉ chạy một lần. Kết quả: 5 + 4 − 1 + 1 = **9 cặp**.

```python
def run_list(cfg):
    runs = [(app, sf, storage, shuffle, rep)
            for app in cfg["apps"]                             # 3 app
            for sf in cfg["sizes"]                             # × số kích thước
            for storage, shuffle in sorted(configurations(cfg))   # × 9 cặp
            for rep in range(1, cfg["repetitions"] + 1)]       # × số lần lặp
    random.Random(cfg["seed"]).shuffle(runs)                   # xáo trộn, cùng seed → cùng thứ tự
    return runs
```
- Ví dụ 4 kích thước × 3 lần lặp: 3 × 4 × 9 × 3 = **324 lần chạy**.
- **Vì sao xáo trộn:** nếu chạy hết gzip rồi mới đến lz4, một lúc cluster chậm (máy khác trên AWS dùng chung phần cứng, mạng…) sẽ rơi trọn vào một codec, làm codec đó có vẻ chậm. Xáo trộn rải các lúc chậm đều lên mọi codec. `sorted(...)` trước khi xáo để thứ tự đầu vào luôn giống nhau (set không có thứ tự cố định), nhờ vậy kết quả xáo trộn tái lập được.

```python
def run_id(app, sf, storage, shuffle, rep):
    return f"{app}_sf{sf:g}_{storage}_{shuffle}_r{rep}"     # io_bound_sf1_zstd_off_r2
```
`{sf:g}` in số gọn: 1 → `1`, 0.1 → `0.1`.

### 3.3 Lệnh chạy một app

```python
def submit_command(cfg, app, sf, storage, shuffle, output):
    if shuffle == "off":
        shuffle_conf = {"spark.shuffle.compress": "false", "spark.shuffle.spill.compress": "false"}
    else:
        shuffle_conf = {"spark.shuffle.compress": "true", "spark.shuffle.spill.compress": "true",
                        "spark.io.compression.codec": shuffle}
    cmd = ["spark-submit", "--class", CLASSES[app]]
    for key, value in shuffle_conf.items():
        cmd += ["--conf", f"{key}={value}"]
    data = f"{DATA}/sf{sf:g}/{storage}"                                # chọn BẢN dữ liệu theo codec lưu trữ
    if app == "balanced":
        args = [f"{data}/lineitem", f"{data}/orders", output, storage]
    else:
        args = [f"{data}/lineitem", output, storage]
    rounds = cfg.get("rounds", {}).get(app)
    return cmd + [JAR] + args + ([str(rounds)] if rounds else [])
```

Ba cấu hình Spark về nén shuffle:
| Cấu hình | Ý nghĩa |
|---|---|
| `spark.shuffle.compress` | nén file shuffle (dữ liệu map ghi ra rồi gửi cho reduce) |
| `spark.shuffle.spill.compress` | nén dữ liệu **spill** (khi thiếu RAM, Spark tạm đổ dữ liệu trung gian xuống đĩa) |
| `spark.io.compression.codec` | codec dùng cho cả hai: `lz4`, `snappy`, `zstd` (và `lzf`) |

Ví dụ lệnh được tạo ra (E1, zstd lưu trữ, tắt nén shuffle):
```
spark-submit --class compression.apps.IoBound
  --conf spark.shuffle.compress=false --conf spark.shuffle.spill.compress=false
  java/build/cloud-bigdata-project.jar
  hdfs://master:9000/data/sf1/zstd/lineitem hdfs://master:9000/results/io_bound_sf1_zstd_off_r2 zstd
```

### 3.4 Gọi lệnh trên các máy

```python
def cluster_nodes():
    if Path(WORKERS_FILE).exists():
        return ["master"] + Path(WORKERS_FILE).read_text().split(), True   # ["master","worker1",…], dùng SSH
    return ["master"], False

NODES, USE_SSH = cluster_nodes()

def on_node(node, command):
    argv = ["ssh", node, command] if USE_SSH else ["sh", "-c", command]
    return subprocess.run(argv, capture_output=True, text=True)
```
`on_node("worker1", "…")` = `ssh worker1 "…"`. Cả local và EC2 đều có file `workers` nên đều đi qua SSH; nhánh `sh -c` chỉ dùng khi không có file đó.

### 3.5 Một lần đo: `run_once`

```python
output = f"{OUTPUT}/{name}"                     # hdfs://master:9000/results/io_bound_sf1_zstd_off_r2
sar_file = f"/tmp/sar-{name}.bin"
for node in NODES:
    if on_node(node, "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches").returncode != 0:
        print(f"  warning: could not drop the page cache on {node}")
    on_node(node, f"nohup sar -o {sar_file} 1 >/dev/null 2>&1 &")
```
- **Xoá page cache:** Linux giữ nội dung file vừa đọc trong RAM để lần sau đọc nhanh. Nếu không xoá, lần đo sau đọc dữ liệu từ RAM, không chạm đĩa, và lợi ích của nén lưu trữ (ít byte đọc từ đĩa) biến mất.
  - `sync`: ghi hết dữ liệu đang chờ xuống đĩa trước.
  - `echo 3 > /proc/sys/vm/drop_caches`: bảo kernel bỏ page cache và cache metadata. Cần quyền root nên dùng `sudo tee`.
  - Trong container không được phép (lỗi → chỉ in cảnh báo); trên EC2 thì được.
- **Bật `sar`:** `sar -o <file> 1` ghi số liệu hệ thống **mỗi giây** vào file nhị phân. `nohup … &`: chạy nền, không bị dừng khi SSH kết thúc. `>/dev/null 2>&1`: bỏ output ra màn hình.

```python
start = time.time()
proc = subprocess.run(submit_command(...), capture_output=True, text=True)    # chạy app, chờ xong
wall_s = round(time.time() - start, 3)                                        # thời gian đồng hồ (giây)
```
`wall_s` gồm cả thời gian khởi động JVM, xin executor… (lớn hơn `duration_ms` của History Server một chút).

```python
sar_texts = {}
for node in NODES:
    on_node(node, "pkill -INT -x sar; sleep 1")                         # dừng sar (gửi Ctrl-C), đợi nó ghi xong
    sar_texts[node] = {kind: on_node(node, f"sadf -d {sar_file} -- {flags}").stdout
                       for kind, flags in metrics.SAR_REPORTS.items()}  # xuất 3 bảng CSV
    on_node(node, f"rm -f {sar_file}")
    … ghi vào results/<env>/sar/<lần đo>/<máy>-cpu.csv, -disk.csv, -net.csv
```
`sadf -d <file> -- <loại>`: đọc file nhị phân của sar, xuất **CSV phân cách bằng `;`**, một dòng mỗi giây. Ba loại (`metrics.SAR_REPORTS`): `-u` CPU, `-b` đĩa, `-n DEV` mạng (xem mục 4.2).

```python
row = {"run_id": name, "app": app, "sf": sf, "storage": storage, "shuffle": shuffle, "wall_s": wall_s}
if proc.returncode != 0:                                       # app lỗi
    (out_dir / "logs" / f"{name}.log").write_text(proc.stderr) # lưu log lỗi
    return {**row, "status": "failed"}
app_id = next(line.split("=")[1] for line in proc.stdout.splitlines() if line.startswith("APP_ID="))
du = subprocess.run(["hdfs", "dfs", "-du", "-s", output], …).stdout        # kích thước output
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", output], …)        # xoá output (không đầy đĩa)
return {**row, "status": "ok", "output_hdfs_bytes": int(du.split()[0]),
        **metrics.spark_metrics(HISTORY, app_id), **metrics.sar_summary(sar_texts)}
```
`{**a, **b}`: gộp hai dict thành một.

### 3.6 `main()`: điều phối

```python
cfg = yaml.safe_load(args.config.read_text())
runs_csv = out_dir / "runs.csv"
old = out_dir / "config.yaml"
if runs_csv.exists() and old.exists() and yaml.safe_load(old.read_text()).get("rounds") != cfg.get("rounds"):
    raise SystemExit(...)                        # đổi rounds = đổi workload → không trộn kết quả
old.write_text(args.config.read_text())          # lưu YAML cạnh kết quả (report.py đọc lại)
```

```python
done = set()
if runs_csv.exists():
    done = {r["run_id"] for r in csv.DictReader(f) if r["status"] == "ok"}    # các lần đã thành công
else:
    csv.writer(f).writerow(COLUMNS)                                            # tạo file với dòng tiêu đề
todo = [r for r in run_list(cfg) if run_id(*r) not in done][:args.limit]
```
**Chạy tiếp được:** lần nào đã `ok` thì bỏ qua. Bị ngắt → chạy lại đúng lệnh. Lần `failed` sẽ được chạy lại.

```python
for app in cfg["apps"]:                                      # khởi động: mỗi app 1 lần, không ghi
    subprocess.run(submit_command(cfg, app, min(cfg["sizes"]), "none", "off", …))
```
Lần chạy đầu tiên thường chậm hơn (JVM nạp class, HDFS nạp metadata…). Chạy bỏ một lần để lần đo đầu tiên không bị thiệt.

```python
for i, (app, sf, storage, shuffle, rep) in enumerate(todo, 1):
    row = run_once(...)
    row["rep"] = rep
    csv.DictWriter(f, fieldnames=COLUMNS).writerow(row)       # GHI NGAY sau mỗi lần (không mất khi bị ngắt)
    print(f"[{i}/{len(todo)}] {name}: {row['status']} in {row['wall_s']} s", flush=True)
    time.sleep(3)                                             # nghỉ 3 giây cho cluster ổn định
```

## 4. `bench/metrics.py`: lấy số đo

### 4.1 Từ Spark History Server

```python
def spark_metrics(history_url, app_id, timeout_s=180):
    url = f"{history_url}/api/v1/applications/{app_id}"
    deadline = time.time() + timeout_s
    while True:
        try:
            app = get_json(url)                              # GET …/applications/<id>
            if app["attempts"][-1]["completed"]:             # đã đọc xong event log của app?
                stages = get_json(url + "/stages")           # GET …/applications/<id>/stages
                break
        except Exception:                                    # 404: History Server chưa thấy app
            pass
        if time.time() > deadline:
            raise TimeoutError(...)
        time.sleep(2)
```
History Server quét thư mục event log định kỳ (mặc định 10 giây), nên ngay sau khi app xong có thể chưa có dữ liệu. Script hỏi lại mỗi 2 giây, tối đa 3 phút.

Thử bằng tay (trên master): `curl -s http://master:18080/api/v1/applications | head` (danh sách app), `curl -s http://master:18080/api/v1/applications/<id>/stages`.

```python
row = {"app_id": app_id, "duration_ms": app["attempts"][-1]["duration"]}   # thời gian app, theo Spark
counted = [s for s in stages if s["status"] in ("COMPLETE", "FAILED")]    # bỏ stage SKIPPED
for field, column in STAGE_FIELDS.items():
    row[column] = sum(s.get(field, 0) for s in counted)                   # cộng qua mọi stage
```
Stage `SKIPPED`: Spark dùng lại kết quả có sẵn, không chạy, nên không tính.

| Trường của History Server | Cột `runs.csv` | Ý nghĩa | Đơn vị |
|---|---|---|---|
| `duration` (của app) | `duration_ms` | thời gian chạy của app → **đồ thị thời gian** | ms |
| `executorRunTime` | `executor_run_ms` | tổng thời gian các task chạy (cộng mọi task) | ms |
| `executorCpuTime` | `executor_cpu_ns` | tổng thời gian **CPU** các task dùng → **đồ thị CPU** | ns |
| `jvmGcTime` | `gc_ms` | thời gian dọn bộ nhớ của Java | ms |
| `inputBytes` | `input_bytes` | byte đọc từ HDFS (**đã nén**, nếu file nén) | byte |
| `outputBytes` | `output_bytes` | byte ghi ra HDFS (1 bản) | byte |
| `shuffleWriteBytes` | `shuffle_write_bytes` | byte shuffle ghi xuống đĩa local (**sau nén**) | byte |
| `shuffleReadBytes` | `shuffle_read_bytes` | byte shuffle đọc (local + qua mạng) | byte |
| `shuffleRemoteBytesRead` | `shuffle_remote_read_bytes` | phần shuffle đọc **qua mạng** | byte |
| `shuffleFetchWaitTime` | `shuffle_fetch_wait_ms` | thời gian task **chờ** dữ liệu shuffle đến (mạng chậm thì cao) | ms |
| `shuffleWriteTime` | `shuffle_write_ns` | thời gian ghi shuffle (gồm cả nén) | ns |
| `diskBytesSpilled` | `disk_spill_bytes` | byte tràn xuống đĩa vì thiếu RAM | byte |
| `memoryBytesSpilled` | `memory_spill_bytes` | kích thước trong RAM của phần đã tràn | byte |

**`executorCpuTime` đo thế nào:** mỗi task, Spark đọc *thread CPU time* của luồng chạy task (thời gian CPU thật sự làm việc, không tính lúc chờ đĩa/mạng) qua `ThreadMXBean` của JVM, rồi cộng lại. Chi phí nén/giải nén chạy trong luồng của task nên nằm trong con số này.

### 4.2 Từ `sar`

```python
SAR_REPORTS = {"cpu": "-u", "disk": "-b", "net": "-n DEV"}
```

Dữ liệu thật (một máy, mỗi giây một dòng):
```
# hostname;interval;timestamp;CPU;%user;%nice;%system;%iowait;%steal;%idle
worker1;1;2026-11-26 05:23:29 UTC;-1;39.45;0.00;4.52;0.00;0.00;56.03

# hostname;interval;timestamp;tps;rtps;wtps;dtps;bread/s;bwrtn/s;bdscd/s
worker1;1;2026-11-26 05:23:29 UTC;13.00;0.00;13.00;0.00;0.00;137.00;0.00

# hostname;interval;timestamp;IFACE;rxpck/s;txpck/s;rxkB/s;txkB/s;rxcmp/s;txcmp/s;rxmcst/s;%ifutil
worker1;1;2026-11-26 05:23:29 UTC;lo;0.00;0.00;0.00;0.00;0.00;0.00;0.00;0.00
worker1;1;2026-11-26 05:23:29 UTC;eth0;1.00;1.00;0.06;0.06;0.00;0.00;0.00;0.00
```
| Cột | Ý nghĩa |
|---|---|
| `%user`, `%nice`, `%system` | % CPU chạy chương trình, chương trình ưu tiên thấp, kernel |
| `%iowait` | % CPU **rảnh vì chờ đĩa** |
| `%idle` | % CPU rảnh |
| `bread/s`, `bwrtn/s` | khối đọc / ghi đĩa mỗi giây (**1 khối = 512 byte**) |
| `IFACE` | card mạng (`lo` = nội bộ trong máy, bỏ qua; `eth0`/`ens5` = card thật) |
| `rxkB/s`, `txkB/s` | kB nhận / gửi mỗi giây |

```python
def parse_sadf(text):
    rows, header = [], None
    for line in text.splitlines():
        if line.startswith("#"):
            header = line[1:].strip().split(";")               # dòng tiêu đề
        elif line.strip() and header:
            rows.append(dict(zip(header, line.split(";"))))    # mỗi dòng → dict {tên cột: giá trị}
    return rows
```

```python
def sar_summary(per_node):
    for texts in per_node.values():                                          # mỗi máy
        cpu = parse_sadf(texts["cpu"])
        busy.append(sum(%user + %nice + %system) / số giây)                  # % CPU bận trung bình của máy
        iowait.append(sum(%iowait) / số giây)
        for r in parse_sadf(texts["disk"]):                                  # mỗi giây
            disk_read_bytes  += bread/s × 512        # tốc độ × 1 giây = số byte trong giây đó
            disk_write_bytes += bwrtn/s × 512
        for r in parse_sadf(texts["net"]):
            if r["IFACE"] != "lo":
                net_rx_bytes += rxkB/s × 1024
                net_tx_bytes += txkB/s × 1024
    cpu_busy_pct = trung bình các máy ; cpu_iowait_pct = trung bình các máy
    (byte đĩa, byte mạng: cộng mọi máy)
```
Vì sar lấy mẫu **mỗi 1 giây**, "tốc độ × 1 giây" cộng qua mọi giây = **tổng số byte** trong cả lần chạy. Đây là số liệu cho **đồ thị đĩa** và **đồ thị mạng**.

### 4.3 Một dòng `runs.csv` thật

```
run_id,status,app,sf,storage,shuffle,rep,wall_s,output_hdfs_bytes,app_id,duration_ms,executor_run_ms,executor_cpu_ns,…
balanced_sf0.1_none_off_r1,ok,balanced,0.1,none,off,1,11.558,3446205,app-20261126051413-0004,10449,13550,10255599645,…
```
Đọc: app `balanced`, SF 0.1, không nén, chạy 11,6 s (đồng hồ) / 10,4 s (Spark), output 3,4 MB, CPU 10,3 s…

## 5. Micro-benchmark: `CodecBench.java`, `codec_bench.sh`

Đo **đặc tính riêng** của từng thuật toán (§4 của đề): tỉ lệ nén, thời gian nén, thời gian giải nén. Một máy, trong RAM: không có HDFS, mạng, Spark.

**`codec_bench.sh`**
```bash
mkdir -p "$2"
hdfs dfs -get -f "$1" /tmp/lineitem.csv                                   # tải 1 file lineitem (không nén) từ HDFS
java -Xmx2g -cp "$SPARK_HOME/jars/*" bench/CodecBench.java "$2/raw.csv" /tmp/lineitem.csv
```
`java <file>.java`: Java 11 chạy được file nguồn trực tiếp, không cần `javac`. `-Xmx2g`: tối đa 2 GB heap. `-cp`: thư viện Spark (chứa các codec).

**`CodecBench.java`**
```java
static final String[] CODECS = {"lz4", "snappy", "lzf", "zstd-1", "zstd-3", "zstd-9", "gzip-6"};
```
zstd ở 3 mức (1 = mức Spark dùng cho shuffle, 3 = mức Hadoop dùng cho file, 9 = mức cao) để thấy đánh đổi tốc độ / tỉ lệ. gzip mức 6 (mặc định).

```java
static OutputStream compress(String codec, OutputStream out) throws IOException {
    if (codec.startsWith("gzip")) return new GZIPOutputStream(out, 65536) { { def.setLevel(6); } };
    return sparkCodec(codec).compressedOutputStream(out);
}
static CompressionCodec sparkCodec(String codec) {
    SparkConf conf = new SparkConf(false);
    if (codec.equals("lz4")) return new LZ4CompressionCodec(conf);
    …
    conf.set("spark.io.compression.zstd.level", codec.substring(5));   // "zstd-3" → mức 3
    return new ZStdCompressionCodec(conf);
}
```
Dùng **chính các lớp codec của Spark** (đúng code nén shuffle), gzip thì dùng `java.util.zip` của JDK. Mỗi codec bọc một luồng ghi (`OutputStream`): ghi dữ liệu vào là ra dữ liệu đã nén.

```java
static long[] roundTrip(String codec, byte[] data) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();          // nén vào RAM
    long t0 = System.nanoTime();
    try (OutputStream out = compress(codec, buffer)) {
        for (int off = 0; off < data.length; off += 65536) {
            out.write(data, off, Math.min(65536, data.length - off));    // ghi từng khúc 64 KB, như khi ghi file
        }
    }
    long compressNs = System.nanoTime() - t0;                            // thời gian nén

    byte[] compressed = buffer.toByteArray();                            // compressed.length = kích thước sau nén
    t0 = System.nanoTime();
    byte[] restored;
    try (InputStream in = decompress(codec, new ByteArrayInputStream(compressed))) {
        restored = in.readAllBytes();                                    // giải nén
    }
    long decompressNs = System.nanoTime() - t0;
    if (!Arrays.equals(data, restored)) throw new IOException(...);      // kiểm tra giải nén đúng
    return new long[] {compressed.length, compressNs, decompressNs};
}
```
`System.nanoTime()`: đồng hồ độ phân giải nano giây. Vì codec chạy một luồng và mọi thứ trong RAM, thời gian đồng hồ ≈ thời gian CPU.

```java
byte[] random = new byte[64 * 1024 * 1024];
new Random(42).nextBytes(random);                        // 64 MB byte ngẫu nhiên: KHÔNG nén được
inputs.put("csv", Files.readAllBytes(Paths.get(args[1])));   // file lineitem thật
inputs.put("random", random);
for (String input : inputs.keySet())
    for (String codec : CODECS) {
        roundTrip(codec, data); roundTrip(codec, data);  // 2 lần khởi động (JIT của Java tối ưu code)
        for (int i = 0; i < 5; i++) {                    // 5 lần đo → raw.csv
            long[] r = roundTrip(codec, data);
            csv.println(input + "," + codec + "," + i + "," + data.length + "," + r[0] + "," + r[1] + "," + r[2]);
        }
    }
```
Đầu vào `random` trả lời: codec tốn bao nhiêu khi nén **vô ích** (dữ liệu đã nén sẵn, ảnh, video), liên quan câu hỏi 7 "khi nào nên tắt nén". `raw.csv`: `input,codec,iteration,input_bytes,compressed_bytes,comp_wall_ns,decomp_wall_ns`.

## 6. Kết quả lưu ở đâu

```
results/<env>/
├── config.yaml              bản sao YAML đã dùng
├── runs.csv                 1 dòng mỗi lần đo (mục 4.3)
├── sar/<lần đo>/<máy>-{cpu,disk,net}.csv   số liệu từng giây
├── logs/<lần đo>.log        log lỗi (chỉ khi failed)
├── datasets/sf<N>.txt       dung lượng mỗi bản dataset (hdfs dfs -du)
├── codec_bench/raw.csv      micro-benchmark
└── run.log                  (AWS) toàn bộ output của run_all.sh
```
