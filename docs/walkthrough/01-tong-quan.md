# 01 — Tổng quan: ý tưởng, khái niệm, kiến trúc, một lần đo đi qua những gì

Đề bài và từng yêu cầu: xem [00-de-bai.md](00-de-bai.md). Phần này đi vào cách project giải quyết.

## 1. Project trả lời câu hỏi gì

Trong một hệ Big Data, dữ liệu liên tục được **đọc từ đĩa**, **gửi qua mạng** và **ghi xuống đĩa**. Nén dữ liệu làm số byte nhỏ đi, nên đọc/ghi/gửi ít hơn. Nhưng nén và giải nén **tốn CPU**. Vậy:

> Nén làm ứng dụng **nhanh hơn hay chậm hơn**? Thuật toán nào, ở mức nào, với loại ứng dụng nào, với dữ liệu lớn cỡ nào?

Project trả lời bằng **đo đạc thật**: chạy cùng một ứng dụng nhiều lần, mỗi lần đổi đúng **một** thứ (thuật toán nén, hoặc nơi nén), đo thời gian, CPU, đĩa, mạng, rồi so sánh.

## 2. Khái niệm nền (cần nắm trước khi đọc code)

### 2.1 HDFS (Hadoop Distributed File System)

- Hệ thống file trải trên nhiều máy. Với người dùng, nó giống một ổ đĩa: đường dẫn kiểu `hdfs://master:9000/data/sf1/gzip/lineitem/part-00000.gz`.
- **NameNode** (chạy trên máy `master`): giữ "mục lục": file nào gồm những block nào, block nằm trên máy nào. Không giữ dữ liệu.
- **DataNode** (chạy trên mỗi máy `worker`): giữ dữ liệu thật, dưới dạng các file block trong `/data/hdfs/datanode`.
- **Block**: file lớn bị cắt thành các khúc 128 MB (mặc định). File nhỏ hơn 128 MB thì chỉ có 1 block.
- **Replication** (bản sao): mỗi block được lưu **3 bản** trên 3 DataNode khác nhau (mặc định). Mất một máy không mất dữ liệu. Hệ quả: ghi 1 GB vào HDFS là ghi 3 GB xuống đĩa, và 2 GB phải đi qua mạng sang máy khác.
- Lệnh hay dùng: `hdfs dfs -ls <thư mục>` (liệt kê), `hdfs dfs -du -s <thư mục>` (dung lượng), `hdfs dfs -rm -r <thư mục>` (xoá), `hdfs dfsadmin -report` (tình trạng các DataNode).

### 2.2 Spark (chế độ standalone)

- **Spark Master** (trên `master`): quản lý tài nguyên của cluster, biết worker nào còn bao nhiêu core/RAM.
- **Spark Worker** (trên mỗi `worker`): nhận lệnh từ Master, khởi chạy **executor**.
- **Driver**: chương trình `main()` của ứng dụng Java. Chạy trên máy gõ `spark-submit` (ở đây là `master`). Driver lập kế hoạch và chia việc.
- **Executor**: tiến trình Java trên worker, làm việc thật (đọc dữ liệu, tính, ghi). Mặc định: mỗi worker một executor, dùng hết core của worker, 1 GB heap.
- **RDD**: tập dữ liệu chia thành nhiều **partition** nằm rải trên các executor. Mỗi partition được xử lý bởi một **task** (một luồng trên một core).
- **Transformation**: phép biến đổi RDD (`map`, `mapToPair`, `flatMap`, `sortByKey`, `reduceByKey`, `join`…). *Lazy*: chưa chạy gì cho đến khi gặp **action**.
- **Action**: phép bắt Spark chạy thật (`saveAsTextFile`, `count`…).
- **Narrow vs wide**:
  - *narrow* (`map`, `mapToPair`, `flatMap`): mỗi partition đầu ra chỉ cần 1 partition đầu vào → làm tại chỗ, không truyền dữ liệu.
  - *wide* (`sortByKey`, `reduceByKey`, `join`): cần gom các phần tử **cùng khoá** về cùng một chỗ → phải **shuffle**.
- **Shuffle** (trộn lại dữ liệu giữa các máy) gồm 2 nửa:
  1. **shuffle write**: mỗi task của bước trước chia dữ liệu của nó theo khoá thành N phần và ghi **xuống đĩa local** của worker (thư mục tạm, mặc định `/tmp`);
  2. **shuffle read (fetch)**: mỗi task của bước sau đi **lấy** phần của mình từ mọi worker, **qua mạng** (hoặc đọc local nếu cùng máy).
- **Stage**: Spark cắt chương trình tại mỗi shuffle. Một app có shuffle → ít nhất 2 stage.
- **Spark History Server**: sau khi app chạy xong, đọc **event log** (nhật ký sự kiện Spark ghi lại trong lúc chạy) và trả lời qua một REST API: app này chạy bao lâu, mỗi stage tốn bao nhiêu CPU, ghi/đọc bao nhiêu byte shuffle… Đây là nguồn số đo chính của project.

### 2.3 Nén

- **Codec** (*coder-decoder*): một thuật toán nén + giải nén.
- **Không mất mát**: giải nén ra đúng từng byte ban đầu (mọi codec ở đây đều vậy).
- **Tỉ lệ nén** = kích thước gốc / kích thước sau nén. 3× nghĩa là còn một phần ba.
- **Tốc độ nén/giải nén**: MB dữ liệu gốc xử lý được mỗi giây, trên 1 core.
- Các codec trong project:

| Codec | Đặc điểm | Có ở mức lưu trữ | Có ở mức shuffle |
|---|---|---|---|
| none / off | không nén (mốc so sánh) | `none` | `off` |
| Gzip | nén mạnh (~3,3×), chậm (~27 MB/s) | ✅ | ❌ Spark không hỗ trợ |
| Snappy | nhanh, nén vừa (~2×) | ✅ | ✅ |
| LZ4 | rất nhanh, nén vừa (~1,9×) | ✅ | ✅ (mặc định của Spark) |
| Zstd | nén mạnh (~3×), khá nhanh ở mức thấp | ✅ (cần thư viện native, chỉ trên EC2) | ✅ |

- **Splittable** (chia nhỏ được): file text nén bằng các codec trên **không** chia nhỏ được, vì phải giải nén từ đầu file. Spark đọc mỗi file nén bằng đúng 1 task, dù file lớn bao nhiêu. File không nén thì cắt theo block (mỗi block 1 task).

### 2.4 Hai mức nén mà đề yêu cầu

| | Nén **lưu trữ** (§5.1) | Nén **truyền thông** (§5.2) |
|---|---|---|
| Nén cái gì | file dữ liệu nằm trên HDFS (input và output) | dữ liệu shuffle (ghi xuống đĩa local và gửi qua mạng) |
| Ai nén | thư viện codec của **Hadoop** | thư viện codec của **Spark** |
| Bật thế nào | **ghi**: `rdd.saveAsTextFile(path, GzipCodec.class)`. **Đọc**: Hadoop nhìn đuôi file (`.gz`, `.snappy`, `.lz4`, `.zst`) để tự chọn codec | khi chạy app: `--conf spark.shuffle.compress=true --conf spark.io.compression.codec=lz4` |
| Lợi gì | ít byte đọc/ghi đĩa, ít chỗ trên HDFS, ít byte nhân bản qua mạng | ít byte ghi shuffle xuống đĩa, ít byte gửi qua mạng |
| Thí nghiệm | **E1**: đổi codec lưu trữ, tắt nén shuffle | **E2**: không nén lưu trữ, đổi codec shuffle |

**E3** (§5.3) kết hợp hai mức: C0 (không nén gì), C1 (chỉ nén lưu trữ), C2 (chỉ nén shuffle), C3 (nén cả hai).

## 3. Kiến trúc

```
                         LAPTOP (macOS)
   make up / run / status / fetch / analysis / shell / down   [ENV=aws]
          │                                    │
          ▼ local (ENV=dev)                    ▼ cloud (ENV=aws)
   Podman: 3 container ubuntu:22.04     AWS Sydney: 1 + N máy EC2 Ubuntu 22.04
   ┌─────────────────────────┐          ┌─────────────────────────┐
   │ master                  │          │ master                  │
   │  NameNode               │          │  NameNode               │
   │  SecondaryNameNode      │          │  SecondaryNameNode      │
   │  Spark Master           │          │  Spark Master           │
   │  History Server         │          │  History Server         │
   │  driver của các app     │          │  driver của các app     │
   │  run_all.sh, run_exp... │          │  run_all.sh (trong tmux)│
   ├─────────────────────────┤          ├─────────────────────────┤
   │ worker1, worker2        │          │ worker1 … workerN       │
   │  DataNode + Spark Worker│          │  DataNode + Spark Worker│
   └─────────────────────────┘          └─────────────────────────┘
   cài bằng cluster/install.sh          cài bằng cluster/install.sh (Ansible chạy)
   khởi động cluster/start-cluster.sh   khởi động cluster/start-cluster.sh
```

- DataNode và Spark Worker nằm **chung máy**: Spark đọc block HDFS ngay trên máy đang có block đó (*data locality*), không phải kéo qua mạng.
- **SecondaryNameNode** (do `start-dfs.sh` tự bật trên master): định kỳ gộp nhật ký thay đổi của NameNode cho gọn. Không phải bản dự phòng; project không dùng tới nó.

## 4. Bản đồ thư mục

```
CloudBigdataProject/
├── Makefile                     mọi lệnh, chạy trên laptop
├── java/                        ỨNG DỤNG SPARK (thứ được đo)
│   ├── build.sh                 javac + jar → java/build/cloud-bigdata-project.jar
│   └── src/main/java/compression/
│       ├── common/Hashing.java  hàm băm tất định + SHA-256 lặp
│       ├── common/Codecs.java   tên codec → lớp codec Hadoop; ghi file
│       ├── datagen/Rows.java    sinh MỘT dòng orders / các dòng lineitem
│       ├── datagen/Generator.java  job Spark sinh cả bảng, ghi mỗi codec một bản
│       ├── apps/IoBound.java    app 1: sắp xếp
│       ├── apps/CpuBound.java   app 2: SHA-256 + gộp
│       └── apps/Balanced.java   app 3: join + gộp
├── bench/                       CÔNG CỤ ĐO (chạy và đo các app)
│   ├── run_all.sh               cả quy trình trên master
│   ├── run_experiments.py       từng lần đo → runs.csv
│   ├── metrics.py               lấy số đo từ History Server và sar
│   ├── CodecBench.java          micro-benchmark codec
│   ├── codec_bench.sh           chạy CodecBench
│   ├── experiments-dev.yaml     thiết kế thí nghiệm (local)
│   └── experiments-aws.yaml     thiết kế thí nghiệm (AWS)
├── analysis/report.py           ĐỒ THỊ + BẢNG từ kết quả
├── cluster/                     DÙNG CHUNG local và EC2
│   ├── install.sh               cài Java, Hadoop, Spark, cấu hình
│   ├── start-cluster.sh         khởi động HDFS + Spark
│   ├── hadoop/                  core-site.xml, hdfs-site.xml, hadoop-env.sh
│   └── spark/                   spark-defaults.conf, spark-env.sh
├── dev/                         CLUSTER LOCAL (Podman)
│   └── Dockerfile, compose.yml, entrypoint.sh, workers
├── infra/
│   ├── terraform/               tạo máy EC2 (main.tf, variables.tf, versions.tf)
│   └── ansible/                 cài lên máy EC2 (cluster.yml, ansible.cfg)
├── results/<env>/               KẾT QUẢ THÔ (runs.csv, sar/, datasets/, codec_bench/)
└── docs/                        tài liệu
```

## 5. Toàn bộ quy trình, từ lệnh đầu tiên đến đồ thị

### 5.1 Ở local

```
make up
  └─ podman compose up --build
       ├─ build image (lần đầu): Dockerfile
       │     FROM ubuntu:22.04 → COPY cluster/ → bash cluster/install.sh
       │        (apt-get, tải + giải nén Hadoop/Spark, biến môi trường, cấu hình: như EC2)
       │     → khoá SSH chung, file workers, chạy daemon bằng root
       └─ tạo 3 container: master, worker1, worker2
            mỗi container: entrypoint.sh → bật SSH server
            master thêm: chờ SSH của worker → bash cluster/start-cluster.sh
                          (format NameNode lần đầu, start-dfs.sh, start-master.sh,
                           start-workers.sh, start-history-server.sh)

make run
  └─ podman compose exec master bash -l bench/run_all.sh dev
       1. bash java/build.sh                         → jar
       2. với mỗi kích thước trong experiments-dev.yaml:
            spark-submit … Generator <sf> hdfs://master:9000/data none,gzip,snappy,lz4
            hdfs dfs -du -s …  > results/dev/datasets/sf<sf>.txt
       3. bash bench/codec_bench.sh …                → results/dev/codec_bench/raw.csv
       4. python3 bench/run_experiments.py bench/experiments-dev.yaml results/dev
            → results/dev/runs.csv (1 dòng mỗi lần đo), results/dev/sar/<lần đo>/…

make analysis
  └─ .venv/bin/python analysis/report.py results/dev  → analysis/figures/dev/
```

### 5.2 Trên AWS

```
make up ENV=aws
  ├─ tạo ~/.ssh/cloud-bigdata-project (nếu chưa có)
  ├─ terraform init + apply   (IP của laptop lấy từ checkip.amazonaws.com)
  │     → VPC, subnet, internet gateway, route table, security group, key pair, 1+N máy EC2
  │     → infra/ansible/inventory.ini
  └─ ansible-playbook cluster.yml
        chờ SSH → tên máy, /etc/hosts, tắt cập nhật tự động
        → copy cluster/ → bash /opt/cluster/install.sh ubuntu (mọi máy)
        → file workers → SSH không mật khẩu từ master
        → bash -l /opt/cluster/start-cluster.sh (master)
        → copy java/ bench/ → build jar

make run ENV=aws     → SSH master: tmux → bash bench/run_all.sh aws (như local, nhưng 5 codec, nhiều kích thước)
make status ENV=aws  → SSH master: đọc log, đếm số dòng ok trong runs.csv
make fetch ENV=aws   → rsync master:/opt/work/results/aws/ → results/aws/
make analysis ENV=aws
make down ENV=aws    → terraform destroy
```

## 6. "Cuộc đời" của MỘT lần đo

Ví dụ lần đo `io_bound_sf1_zstd_off_r2`: ứng dụng I/O-bound, dữ liệu SF 1, lưu trữ nén zstd, shuffle không nén, lần lặp thứ 2.

```
run_experiments.py (trên master)
 1. Trên mọi máy (qua SSH):
      sync; echo 3 | sudo tee /proc/sys/vm/drop_caches     ← xoá page cache (bộ nhớ đệm file của Linux)
      nohup sar -o /tmp/sar-<lần đo>.bin 1 &                ← bắt đầu ghi CPU/đĩa/mạng mỗi giây
 2. Bấm giờ, chạy:
      spark-submit --class compression.apps.IoBound
                   --conf spark.shuffle.compress=false --conf spark.shuffle.spill.compress=false
                   java/build/cloud-bigdata-project.jar
                   hdfs://master:9000/data/sf1/zstd/lineitem      ← input: bản nén zstd
                   hdfs://master:9000/results/io_bound_sf1_zstd_off_r2   ← output
                   zstd                                            ← output cũng nén zstd
      ├─ driver in "APP_ID=app-2026…" ra màn hình
      ├─ Spark đọc các file .zst (Hadoop tự giải nén), shuffle KHÔNG nén, ghi output .zst
      └─ Spark ghi event log vào hdfs://master:9000/spark-logs
 3. Dừng giờ → wall_s. Trên mọi máy: dừng sar, xuất CSV bằng sadf (cpu, disk, net),
    lưu vào results/aws/sar/io_bound_sf1_zstd_off_r2/<máy>-{cpu,disk,net}.csv
 4. hdfs dfs -du -s <output> → output_hdfs_bytes ; xoá output
 5. Hỏi History Server (chờ đến khi nó đọc xong event log):
      GET /api/v1/applications/<APP_ID>         → duration_ms
      GET /api/v1/applications/<APP_ID>/stages  → cộng qua các stage: CPU, byte input/output,
                                                  byte shuffle ghi/đọc/qua mạng, thời gian chờ fetch, GC, spill
 6. Tóm tắt sar: %CPU trung bình, %iowait, byte đọc/ghi đĩa, byte mạng nhận/gửi (cộng mọi máy)
 7. Ghi 1 dòng vào results/aws/runs.csv
 8. Nghỉ 3 giây cho cluster "lắng xuống", sang lần đo tiếp theo
```

Sau 324 lần như vậy (cấu hình AWS chính thức), `runs.csv` có đủ dữ liệu để vẽ mọi đồ thị.

## 7. Bảng lệnh

| Lệnh | Làm gì | Mất bao lâu |
|---|---|---|
| `make up` | dựng cluster local (lần đầu: build image) | lần đầu ~5–10 phút, sau đó vài giây |
| `make run` | cả quy trình local | ~15 phút |
| `make status` | số lần đo `ok` | tức thì |
| `make analysis` | đồ thị + bảng (lần đầu tạo `.venv`) | vài giây |
| `make shell` | vào container master | |
| `make down` | tắt container (dữ liệu HDFS giữ lại) | |
| `make up ENV=aws` | tạo + cài cluster EC2 | ~12 phút |
| `make run ENV=aws` | bắt đầu quy trình trên master (tmux) | trả về ngay; thí nghiệm chạy nhiều giờ |
| `make status ENV=aws` | log + số lần `ok` | |
| `make fetch ENV=aws` | kéo kết quả về | |
| `make shell ENV=aws` | SSH vào master + mở web UI | |
| `make down ENV=aws` | xoá toàn bộ cluster | ~2 phút |

Giao diện web (local, hoặc AWS khi đang `make shell ENV=aws`): HDFS http://localhost:9870, Spark Master http://localhost:8080, History Server http://localhost:18080.
