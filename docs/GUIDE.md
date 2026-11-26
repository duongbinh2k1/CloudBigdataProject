# Hướng dẫn học và bảo vệ project

Tài liệu này giải thích **toàn bộ** project: đề bài yêu cầu gì, ý tưởng chính, kiến trúc, luồng chạy từ lệnh `make` đến đồ thị, và từng file code. Câu hỏi bảo vệ và câu trả lời ở [QA.md](QA.md).

Thứ tự đọc nên theo: mục 1 → 2 → 3 → 4 (hiểu bức tranh chung), rồi mục 5 (code), mục 6–7 (thí nghiệm, kết quả), mục 8 (giới hạn).

---

## 0. Tình trạng hiện tại

| Phần | Trạng thái |
|---|---|
| Code: bộ sinh dữ liệu, 3 ứng dụng, micro-benchmark, script thí nghiệm, phân tích | ✅ chạy đủ trên cluster local (24/24 lần chạy) |
| Hạ tầng AWS: Terraform + Ansible | ✅ viết xong, qua kiểm tra cú pháp; ⬜ chưa chạy thật |
| Số liệu thật trên AWS (`results/aws/`) | ⬜ |
| Báo cáo, slide (trả lời 7 câu hỏi bằng số liệu) | ⬜ |

Cấu hình AWS hiện đang ở chế độ **test** (máy `m7i-flex.large`, SF 1, 1 lần lặp). Số liệu báo cáo phải chạy lại bằng `m5.xlarge`, SF 1/5/10/20, 5 lần lặp (xem [AWS.md](AWS.md)).

---

## 1. Đề bài nói gì

Đề: *InfraCloud Project: Experimental Evaluation of Data Compression in Hadoop or Spark* (`CoursCloudN7/ProjectUSTH.pdf`).

**Câu hỏi nghiên cứu:** nén dữ liệu trong một hệ Big Data có lợi hay có hại, khi nào, và vì sao?

Nén có **lợi**: ít byte hơn phải đọc/ghi đĩa và truyền qua mạng. Nén có **chi phí**: CPU phải nén và giải nén. Kết quả cuối cùng phụ thuộc vào việc ứng dụng đang bị giới hạn bởi cái gì (đĩa, mạng hay CPU).

Đề yêu cầu:

| Mục đề | Nội dung | Trong project |
|---|---|---|
| §2 Hạ tầng | cluster Hadoop/Spark, HDFS, nhiều worker, công cụ giám sát, tái lập được | Spark standalone + HDFS trên 4 máy EC2 (Terraform + Ansible); giám sát bằng `sar` và Spark History Server |
| §3 Ứng dụng | 3 loại: I/O-bound, CPU-bound, cân bằng | `IoBound`, `CpuBound`, `Balanced` (Java, RDD) |
| §4 Thuật toán | Snappy, LZ4, Gzip, Zstd (nếu được hỗ trợ) + không nén | cả 4 + `none`/`off`; đo thêm bằng micro-benchmark |
| §5.1 Nén lưu trữ | nén file trên HDFS | E1 |
| §5.2 Nén truyền thông | nén dữ liệu shuffle | E2 |
| §5.3 Kết hợp | C0–C3 | E3 |
| §5.4 Kích thước | nhiều kích thước dữ liệu | SF 1, 5, 10, 20 (≈ 1–20 GB) |
| §6 Kết quả | lặp nhiều lần, đồ thị (có error bar), trả lời 7 câu hỏi | 5 lần lặp, `analysis/report.py`, báo cáo |
| §7 Nộp | script hạ tầng, số liệu thô, script thí nghiệm, đồ thị, báo cáo, slide | `infra/`, `results/`, `bench/`, `analysis/`, `report/` |

---

## 2. Kiến thức nền cần nắm

### 2.1 HDFS

- Hệ thống file phân tán của Hadoop. **NameNode** (trên master) giữ metadata: file nào gồm những block nào, block nằm ở đâu. **DataNode** (trên mỗi worker) giữ dữ liệu thật.
- File được cắt thành **block 128 MB** (`dfs.blocksize`). Mỗi block có **3 bản sao** (`dfs.replication`) trên 3 DataNode khác nhau.
- Spark đọc HDFS theo block: mỗi block (nếu file chia được) thành một **task**, và Spark cố chạy task trên chính máy chứa block đó (*data locality*). Đó là lý do mỗi worker vừa là DataNode vừa là Spark Worker.

### 2.2 Spark

- **Driver**: chương trình chính (`main` của app Java), lập kế hoạch và điều phối. Ở đây driver chạy trên master (`spark-submit` mặc định *client mode*).
- **Executor**: tiến trình trên worker, chạy các task. Mỗi worker có 1 executor dùng hết core của máy.
- **RDD**: tập dữ liệu phân tán, chia thành **partition**. Biến đổi (`map`, `mapToPair`, `filter`…) là *lazy*, chỉ chạy khi gặp **action** (`saveAsTextFile`, `count`…).
- **Narrow transformation** (`map`, `mapToPair`, `flatMap`): mỗi partition đầu ra chỉ cần một partition đầu vào, chạy tại chỗ.
- **Wide transformation / shuffle** (`sortByKey`, `reduceByKey`, `join`): dữ liệu phải **phân phối lại theo khoá** giữa các máy. Spark cắt chương trình thành **stage** tại mỗi shuffle:
  1. **Shuffle write**: mỗi task ở stage trước ghi dữ liệu đã chia theo khoá xuống **đĩa local** của worker (`SPARK_LOCAL_DIRS`).
  2. **Shuffle read (fetch)**: task ở stage sau tải phần dữ liệu của mình từ mọi worker, qua mạng (hoặc đọc local nếu cùng máy).
- `spark.default.parallelism`: số partition mặc định của mọi shuffle trong RDD.

### 2.3 Nén

- **Tỉ lệ nén** = kích thước gốc / kích thước sau nén (càng lớn càng nén mạnh).
- **Tốc độ nén / giải nén** (MB/s): càng nhanh càng ít tốn CPU.
- Hai họ codec:
  - **Nhanh, nén vừa**: LZ4, Snappy (và LZF). Tỉ lệ ~2× trên dữ liệu của project, vài trăm MB/s.
  - **Nén mạnh, chậm hơn**: Gzip (deflate), Zstd. Tỉ lệ ~3×. Zstd có nhiều mức: mức 1 gần nhanh bằng LZ4, mức cao chậm như Gzip.
- **Chia nhỏ được (splittable)?** File text nén bằng gzip/snappy/lz4/zstd (dạng stream) **không chia nhỏ được**: phải đọc từ đầu file, nên **1 file = 1 task**, dù file lớn bao nhiêu. (bzip2 là ngoại lệ, nhưng rất chậm.)

### 2.4 Hai mức nén trong Spark

| | Nén lưu trữ (§5.1) | Nén truyền thông (§5.2) |
|---|---|---|
| Nén cái gì | file input/output trên HDFS | file shuffle trên đĩa local + dữ liệu truyền giữa các node |
| Ai nén | codec của **Hadoop** (`GzipCodec`, `SnappyCodec`, `Lz4Codec`, `ZStandardCodec`) | codec của **Spark** (`lz4`, `snappy`, `zstd`, `lzf`) |
| Cách bật | ghi: `rdd.saveAsTextFile(path, GzipCodec.class)`; đọc: tự nhận theo đuôi file `.gz`, `.snappy`, `.lz4`, `.zst` | `spark.shuffle.compress=true`, `spark.shuffle.spill.compress=true`, `spark.io.compression.codec=<codec>` |
| Lợi ích | ít byte đọc/ghi đĩa, ít chỗ trên HDFS, ít byte nhân bản 3 lần | ít byte ghi xuống đĩa khi shuffle và ít byte qua mạng |
| Gzip? | có | **không** (Spark không hỗ trợ gzip cho shuffle) |

Mặc định Spark **bật** nén shuffle với LZ4. Trong thí nghiệm, script luôn đặt rõ giá trị (bật với codec cụ thể, hoặc tắt hẳn) để không phụ thuộc vào giá trị mặc định.

---

## 3. Kiến trúc

### 3.1 Trên AWS (số liệu báo cáo)

```
                         Laptop
   make up / run / status / fetch / analysis / down ENV=aws
        │ Terraform (API AWS)   │ Ansible (SSH)    │ SSH, rsync
        ▼                       ▼                  ▼
┌───────────────────── AWS, vùng Sydney, một availability zone ───────────────────────┐
│  master                                                                              │
│    HDFS NameNode · Spark Master · Spark History Server                               │
│    driver của các app · bench/run_all.sh, run_experiments.py (trong tmux)            │
│        │ mạng nội bộ VPC (security group: chỉ các node trong cluster với nhau)        │
│  worker1, worker2, worker3                                                           │
│    HDFS DataNode · Spark Worker (1 executor dùng hết core)                           │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

- 4 máy EC2 cùng loại (hiện tại `m7i-flex.large` để test, `m5.xlarge` cho số liệu chính thức), Ubuntu 22.04, đĩa gp3 200 GB, trong một VPC riêng do Terraform tạo.
- Vùng Sydney (`ap-southeast-2`): vùng duy nhất tài khoản Free plan được phép dùng.
- Ít nhất 2 máy thật để shuffle đi qua **mạng thật** (yêu cầu của thầy). 3 worker để có song song.
- Chỉ IP hiện tại của laptop được SSH vào cluster (security group).

### 3.2 Ở local (phát triển, không dùng số liệu)

```
Laptop ── Podman ── container master  (NameNode, Spark Master, History Server)
                 ├─ container worker1 (DataNode, Spark Worker)
                 └─ container worker2 (DataNode, Spark Worker)
         thư mục project được gắn vào /opt/work của mỗi container
```

Mỗi container đóng vai **một máy Ubuntu**, cài giống hệt máy EC2: `apt` (Java, SSH, sysstat), giải nén Hadoop/Spark, cùng file cấu hình trong `conf/`, SSH không mật khẩu giữa các máy. Master khởi động cả cluster qua SSH bằng `start-dfs.sh`, `start-master.sh`, `start-workers.sh`, đúng như trên EC2 và như slide.

Cùng phần mềm (Java 11, Hadoop 3.4.1, Spark 3.5.5), cùng code, cùng script. Khác: kích thước nhỏ (SF 0.1), không có zstd ở mức lưu trữ (Mac là ARM, thư viện native của Hadoop chỉ có cho x86_64), và 3 container dùng chung một máy nên số đo không có ý nghĩa.

### 3.3 Cấu trúc thư mục

```
java/                       code Spark (Java, RDD)
  src/main/java/compression/
    datagen/Rows.java         sinh một dòng orders / lineitem
    datagen/Generator.java    job Spark sinh toàn bộ dataset, ghi mỗi codec một bản
    common/Hashing.java       hàm băm tất định + SHA-256 lặp
    common/Codecs.java        tên codec → lớp codec Hadoop, ghi file
    apps/IoBound.java         app 1: sắp xếp
    apps/CpuBound.java        app 2: SHA-256 + reduceByKey
    apps/Balanced.java        app 3: join + tổng hợp
  build.sh                  javac + jar → java/build/cloud-bigdata-project.jar
bench/                      chạy thí nghiệm
  run_all.sh                toàn bộ quy trình trên master (dev hoặc aws)
  run_experiments.py        danh sách lần chạy, chạy từng lần, ghi runs.csv
  metrics.py                đọc số đo từ History Server và sar
  CodecBench.java, codec_bench.sh   micro-benchmark codec
  experiments-dev.yaml, experiments-aws.yaml   thiết kế thí nghiệm
analysis/report.py          đồ thị + bảng
conf/                       file cấu hình Hadoop + Spark, dùng chung cho cluster local và EC2
dev/                        cluster local (Dockerfile, compose.yml, entrypoint.sh, workers)
infra/terraform/            tạo máy EC2
infra/ansible/              cài và khởi động HDFS + Spark
Makefile                    mọi lệnh (chạy trên laptop)
results/<env>/              số liệu thô
docs/                       tài liệu
```

---

## 4. Luồng chạy từ đầu đến cuối

Mọi lệnh gõ trên laptop. `ENV=dev` (mặc định) là cluster local, `ENV=aws` là EC2.

```
make up ENV=aws
 ├─ tạo khoá SSH ~/.ssh/cloud-bigdata-project nếu chưa có
 ├─ Terraform: tạo 4 máy EC2 + security group + key pair (với IP hiện tại của laptop)
 │             → ghi infra/ansible/inventory.ini (tên máy, IP public, IP private)
 └─ Ansible (cluster.yml):
      chờ SSH → cài Java, sysstat, libzstd → tải Hadoop + Spark
      → copy file cấu hình conf/ + file workers → kiểm tra zstd (checknative)
      → SSH không mật khẩu từ master → format NameNode → start-dfs.sh
      → start-master.sh, start-workers.sh, start-history-server.sh
      → copy java/ và bench/ lên /opt/work → build jar

make run ENV=aws
 └─ SSH vào master, mở tmux, chạy bench/run_all.sh aws:
      1. build jar
      2. với mỗi kích thước trong YAML: Generator → 5 bản dataset trên HDFS; ghi kích thước (hdfs dfs -du)
      3. micro-benchmark codec (CodecBench.java)
      4. run_experiments.py: 1 lần khởi động mỗi app, rồi từng lần chạy:
           xoá page cache mọi node → bật sar mọi node → spark-submit (app + codec)
           → tắt sar, lấy CSV → hỏi History Server số đo → ghi 1 dòng runs.csv → xoá output

make status ENV=aws     xem tiến độ (log, số lần ok)
make fetch ENV=aws      rsync results/aws về laptop
make analysis ENV=aws   analysis/report.py → analysis/figures/aws/ (đồ thị + bảng)
make down ENV=aws       terraform destroy (xoá hết, ngừng tính tiền)
```

Ở local: `make up` (dựng 3 container Ubuntu; master chạy `start-dfs.sh`, `start-master.sh`, `start-workers.sh`) → `make run` (cùng `run_all.sh`, bản `dev`, trong container master) → `make analysis`.

**Mỗi thứ cấu hình ở một chỗ:**

| Muốn đổi | Sửa | Ai đọc |
|---|---|---|
| Loại máy, số worker, đĩa, vùng | `infra/terraform/variables.tf` | Terraform |
| Hadoop, Spark (dùng chung local và EC2) | `conf/hadoop/`, `conf/spark/` | NameNode, DataNode, Spark Master/Worker, `spark-submit` |
| Thí nghiệm (kích thước, codec, số lần lặp, app, rounds) | `bench/experiments-<env>.yaml` | `run_all.sh`, `run_experiments.py`, `report.py` |

---

## 5. Code chi tiết

### 5.1 Dữ liệu: `Rows`, `Hashing`, `Generator`, `Codecs`

**Hai bảng kiểu TPC-H** (chuẩn benchmark cơ sở dữ liệu), dạng text, cột phân tách bằng `|`:

| Bảng | Số dòng | Cột |
|---|---|---|
| `orders` | 1 500 000 × SF | mã đơn, mã khách, trạng thái, tổng giá, ngày đặt, độ ưu tiên, nhân viên, …, comment |
| `lineitem` | ≈ 6 000 000 × SF (1–7 dòng mỗi đơn) | mã đơn, mã hàng, mã nhà cung cấp, số dòng, số lượng, giá, chiết khấu, thuế, …, ngày giao, ngày cam kết, ngày nhận, …, comment |

SF 1 ≈ 933 MB (orders 176 MB, lineitem 757 MB).

**Ý tưởng 1: tất định.** `Hashing.hash(seed, cột, khoá)` là hàm băm SplitMix64, cho cùng kết quả với cùng đầu vào:

```java
public static long hash(long seed, String column, long key) {
    long h = mix(seed ^ 0x9e3779b97f4a7c15L);
    h = mix(h ^ column.hashCode());
    h = mix(h ^ key);
    return h >>> 1;
}
public static long uniform(long seed, String column, long key, long low, long high) {
    return low + Math.floorMod(hash(seed, column, key), high - low + 1);
}
```

Mỗi ô của bảng = `uniform(42, "tên cột", mã dòng, min, max)`. Không dùng `java.util.Random` (sinh tuần tự), nên **dòng thứ k luôn giống nhau**, dù chạy với bao nhiêu partition, bao nhiêu máy, chạy lại bao nhiêu lần. Thí nghiệm tái lập được tuyệt đối (đã kiểm tra: sinh lại cho cùng số byte).

**Ý tưởng 2: dữ liệu "giống thật" để nén có ý nghĩa.** Ngày 1992–1998, các cột phân loại có ít giá trị (5 độ ưu tiên, 7 phương thức giao), comment ghép từ một bộ từ vựng cố định. Dữ liệu ngẫu nhiên hoàn toàn thì không nén được và không phản ánh dữ liệu doanh nghiệp.

**Ý tưởng 3: không cần join khi sinh.** `Rows.lineItems(seed, sf, orderKey)` tính lại ngày đặt hàng của đơn từ cùng hàm băm, nên `lineitem` luôn khớp với `orders` mà không phải đọc bảng kia.

**`Generator.java`** (job Spark):

```java
long totalOrders = Rows.orderCount(sf);                                   // 1 500 000 × SF
int files = (int) ((totalOrders + ORDERS_PER_FILE - 1) / ORDERS_PER_FILE); // 150 000 đơn / file
JavaRDD<Integer> partitions = sc.parallelize(partitionIds, files);         // partition p = file p

JavaRDD<String> orders = partitions.flatMap(p -> keys(p, totalOrders)
    .mapToObj(key -> Rows.order(SEED, sf, key)).iterator());
JavaRDD<String> lineitem = partitions.flatMap(p -> keys(p, totalOrders)
    .boxed().flatMap(key -> Rows.lineItems(SEED, sf, key).stream()).iterator());

for (String codec : codecs) {
    String dir = base + "/sf" + sfText + "/" + codec;
    Codecs.save(orders, dir + "/orders", codec);
    Codecs.save(lineitem, dir + "/lineitem", codec);
}
```

- `parallelize(0..files-1)`: RDD có mỗi phần tử là số thứ tự partition. Partition p sinh các đơn `[p·150000+1, (p+1)·150000]` (hàm `keys`).
- `flatMap(... .iterator())`: dòng được sinh **dần dần** (lazy), không giữ cả partition trong RAM.
- Mỗi codec ghi một bản: `hdfs://master:9000/data/sf<SF>/<codec>/{orders,lineitem}/part-NNNNN[.gz|.snappy|.lz4|.zst]`.

**Vì sao 150 000 đơn mỗi file?** Vì file nén không chia nhỏ được. Một file `lineitem` 150 000 đơn ≈ 85 MB, **nhỏ hơn 1 block HDFS (128 MB)**:
- bản nén: 1 file = 1 task (không chia được);
- bản `none`: file < 1 block nên cũng = 1 task.

→ **Mọi codec có cùng số task đọc**, nên khi so sánh thì chỉ có nén thay đổi, không phải mức song song. Nếu để file lớn, bản `none` sẽ được chia thành nhiều task hơn và so sánh không công bằng.

**`Codecs.java`**: đổi tên codec thành lớp codec Hadoop, xoá output cũ, ghi:

```java
case "gzip": return GzipCodec.class;      case "snappy": return SnappyCodec.class;
case "lz4":  return Lz4Codec.class;       case "zstd":   return ZStandardCodec.class;
...
if (cls == null) rdd.saveAsTextFile(path); else rdd.saveAsTextFile(path, cls);
```

Khi đọc (`sc.textFile`), Hadoop tự chọn codec theo đuôi file, nên app không cần biết dữ liệu nén bằng gì.

`ZStandardCodec` cần **thư viện native của Hadoop** (`libhadoop.so`) và `libzstd`: chỉ có cho Linux x86_64 → chỉ chạy trên EC2. Ba codec còn lại chạy bằng thư viện Java có sẵn trong Spark.

### 5.2 Ba ứng dụng

Cả ba có cùng khung, giống WordCount trong slide:

```java
JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("io_bound"));
System.out.println("APP_ID=" + sc.sc().applicationId());   // script đọc dòng này để lấy số đo
JavaRDD<String> lines = sc.textFile(input);                  // codec lưu trữ: tự nhận theo đuôi file
... biến đổi ...
Codecs.save(result, output, codec);                          // output ghi bằng cùng codec lưu trữ
```

Tham số theo vị trí: `<thư mục input> <thư mục output> <codec> [rounds]`. Codec **shuffle** không nằm trong code: script truyền bằng `--conf` khi `spark-submit`.

**App 1, `IoBound`: sắp xếp toàn bộ `lineitem` theo ngày giao**

```java
lines.mapToPair(line -> new Tuple2<>(sortKey(line), line))   // khoá = "1996-03-13|000000000042|3"
     .sortByKey(true, sc.defaultParallelism())                // SHUFFLE: toàn bộ dữ liệu
     .values();
```

- Đọc toàn bộ dữ liệu, shuffle **toàn bộ** dữ liệu (mỗi dòng phải đến partition chứa khoảng ngày của nó), ghi lại toàn bộ.
- Mỗi dòng chỉ tách chuỗi và ghép khoá → gần như không tính toán → thời gian dành cho đĩa và mạng.
- `sortByKey` dùng *range partitioning*: trước khi sắp xếp, Spark chạy thêm một job **lấy mẫu** đọc input để chọn ranh giới các khoảng → input được đọc **2 lần**.

**App 2, `CpuBound`: SHA-256 lặp 64 lần mỗi dòng**

```java
.mapToPair(line -> {
    long bucket = Long.parseLong(f[1]) % 1000;                                   // partkey % 1000
    String digest = Hashing.iteratedSha256(f[0] + "|" + f[3] + "|" + f[15], rounds); // 64 vòng
    return new Tuple2<>(bucket, new Tuple2<>(1L, Hashing.prefix(digest)));
})
.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, Math.max(a._2, b._2)), sc.defaultParallelism())
```

- Mỗi dòng tính SHA-256 64 lần → nặng CPU.
- `reduceByKey` **gộp ngay ở phía map** (như *combiner* trong slide Hadoop): mỗi task chỉ gửi tối đa 1000 cặp (bucket, (đếm, max)) → shuffle gần như bằng 0.
- `Hashing.prefix` lấy 15 chữ số hex đầu của digest thành số `long` để lấy `max` bằng số (rẻ) thay vì so chuỗi.

**App 3, `Balanced`: join hai bảng rồi tổng hợp**

```java
lines  = lineitem.mapToPair(l -> (orderkey, (doanh thu, số ngày trễ, comment)))
orders = orders.mapToPair(o -> (orderkey, ("custkey|priority", comment)))
lines.join(orders, partitions)                                      // SHUFFLE cả hai bảng
     .mapToPair(t -> ("custkey|priority", (doanh thu, 1, trễ, max SHA-256 8 vòng)))
     .reduceByKey(cộng, cộng, cộng, max)                             // SHUFFLE nhỏ
```

- `join` phải đưa mọi dòng có cùng mã đơn về cùng partition → shuffle **cả hai bảng** (lớn).
- Có tính toán: tách chuỗi, tính doanh thu, số ngày trễ, SHA-256 8 vòng → vừa I/O vừa CPU.
- Doanh thu = giá × (1 − chiết khấu) × (1 + thuế), tính bằng **số nguyên** (cent × % × %) để tổng chính xác tuyệt đối, không sai số dấu phẩy động.

**Làm sao chứng minh đúng kiểu workload?** Bảng `workload_characterisation.csv` (từ các lần chạy không nén):
- `cpu_ratio` = thời gian CPU / thời gian chạy của task (gần 1 → CPU-bound; thấp → chờ I/O);
- **MB di chuyển trên mỗi giây CPU** = (input + shuffle đọc + shuffle ghi + output) / thời gian CPU. Cao → nặng di chuyển dữ liệu.

Ở dev (SF 1): io_bound 126, balanced 33, cpu_bound 13 MB/giây CPU, tức cách nhau khoảng 3–4 lần mỗi bậc. Số vòng SHA-256 (64 và 8) được chọn để có khoảng cách này: với 32 vòng, cpu_bound (20) quá gần balanced (31).

### 5.3 Micro-benchmark: `CodecBench.java`

Đo **đặc tính riêng** của từng codec, trên một máy, không có HDFS, không có mạng (§4 của đề).

```java
static final String[] CODECS = {"lz4", "snappy", "lzf", "zstd-1", "zstd-3", "zstd-9", "gzip-6"};
...
try (OutputStream out = compress(codec, buffer)) { ...ghi dữ liệu theo khối 64 KB... }   // đo thời gian nén
try (InputStream in = decompress(codec, ...)) { restored = in.readAllBytes(); }        // đo thời gian giải nén
if (!Arrays.equals(data, restored)) throw ...                                           // kiểm tra đúng
```

- Dùng **chính các lớp codec của Spark** (`LZ4CompressionCodec`, `ZStdCompressionCodec`…), tức đúng code nén shuffle; gzip dùng `java.util.zip`.
- Hai đầu vào: một file `lineitem` thật (~75–85 MB text) và 64 MB **byte ngẫu nhiên** (không nén được, để xem codec tốn bao nhiêu CPU khi nén vô ích).
- Mỗi cặp (đầu vào, codec): 2 lần khởi động (JIT của Java), rồi 5 lần đo.
- `codec_bench.sh` tải file từ HDFS về rồi chạy `java ... CodecBench.java` (Java 11 chạy được file `.java` trực tiếp, không cần biên dịch).

### 5.4 `bench/run_all.sh`: cả quy trình trên master

```bash
sizes=$(yaml sizes)                         # đọc từ bench/experiments-<env>.yaml
codecs=$(yaml storage_codecs | tr ' ' ',')
bash java/build.sh                          # 1. build jar
for sf in $sizes; do                        # 2. dữ liệu, bỏ qua nếu đã có
  [ -f $results/datasets/sf$sf.txt ] && continue
  spark-submit --class compression.datagen.Generator ... $sf hdfs://master:9000/data $codecs
  hdfs dfs -du -s "hdfs://master:9000/data/sf$sf/*/*" > $results/datasets/sf$sf.txt
done
[ -f $results/codec_bench/raw.csv ] || bash bench/codec_bench.sh ...   # 3. micro-benchmark
python3 bench/run_experiments.py $config $results                     # 4. thí nghiệm
```

- `set -e`: lỗi ở bước nào thì dừng ngay (ví dụ zstd không chạy được → dừng ở bước 2).
- File `sf<N>.txt` chỉ được ghi khi sinh xong kích thước đó → bị ngắt giữa chừng thì lần sau sinh lại kích thước đó từ đầu.

### 5.5 `bench/run_experiments.py`: chạy các lần đo

**Bước 1. Danh sách cấu hình** (E1, E2, E3, mỗi cặp chỉ một lần):

```python
pairs  = {(s, "off") for s in cfg["storage_codecs"]}      # E1: nén lưu trữ, tắt nén shuffle
pairs |= {("none", s) for s in cfg["shuffle_codecs"]}     # E2: không nén lưu trữ, đổi codec shuffle
x, y = cfg["combined"]["storage"], cfg["combined"]["shuffle"]
pairs |= {("none", "off"), (x, "off"), ("none", y), (x, y)}   # E3: C0, C1, C2, C3
```

Với `experiments-aws.yaml` chính thức: E1 có 5 cặp, E2 có 4, chung nhau `(none, off)` → 8; E3 thêm `(zstd, lz4)` (C1 và C2 đã có trong E1/E2) → **9 cặp**. × 3 app × 4 kích thước × 5 lần lặp = **540 lần chạy**.

**Bước 2. Xáo trộn thứ tự** với seed cố định:

```python
random.Random(cfg["seed"]).shuffle(runs)
```

Nếu chạy hết gzip rồi mới đến lz4, một khoảng thời gian cluster chậm (máy hàng xóm trên AWS, mạng) sẽ rơi trọn vào một codec → kết quả sai lệch. Xáo trộn rải nhiễu đều. Seed cố định → thứ tự tái lập được.

**Bước 3. Khởi động**: mỗi app chạy 1 lần không ghi lại (JVM, cache metadata HDFS, đĩa "nóng" lên).

**Bước 4. Mỗi lần chạy (`run_once`)**:

```python
for node in NODES:
    on_node(node, "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches")   # xoá page cache
    on_node(node, f"nohup sar -o {sar_file} 1 >/dev/null 2>&1 &")      # sar ghi mỗi giây
proc = subprocess.run(submit_command(...))                              # spark-submit
for node in NODES:
    on_node(node, "pkill -INT -x sar; sleep 1")                         # dừng sar
    ... sadf -d ... → results/<env>/sar/<run_id>/<node>-cpu|disk|net.csv
app_id = (dòng "APP_ID=..." trong stdout của app)
row = {..., **metrics.spark_metrics(HISTORY, app_id), **metrics.sar_summary(sar_texts)}
```

- **Xoá page cache**: Linux giữ file vừa đọc trong RAM. Không xoá thì lần chạy sau đọc từ RAM, không chạm đĩa → không đo được lợi ích của nén lưu trữ.
- `NODES`: master + các máy trong `/opt/spark/conf/workers` (file mà `start-workers.sh` dùng; trên EC2 do Ansible ghi, ở local có sẵn trong image), mỗi máy được gọi qua SSH.
- `submit_command` chỉ truyền **codec shuffle** và tham số app; mọi thứ khác là mặc định của Spark.

```python
if shuffle == "off": {"spark.shuffle.compress": "false", "spark.shuffle.spill.compress": "false"}
else:                {"spark.shuffle.compress": "true",  "spark.shuffle.spill.compress": "true",
                      "spark.io.compression.codec": shuffle}
```

**Bước 5. Chạy tiếp được**: `run_id = io_bound_sf5_zstd_off_r3`. Lần nào đã có `status = ok` trong `runs.csv` thì bỏ qua. Thêm kích thước, codec hay số lần lặp → chỉ chạy phần mới. Đổi `rounds` → script **từ chối** chạy (vì sẽ trộn kết quả của hai workload khác nhau).

### 5.6 `bench/metrics.py`: lấy số đo

**Spark History Server** (REST API, đọc event log của app sau khi chạy xong):

| Endpoint | Dùng để |
|---|---|
| `/api/v1/applications/<id>` | thời gian chạy (`duration`) |
| `/api/v1/applications/<id>/stages` | cộng qua mọi stage: `executorRunTime`, `executorCpuTime`, `inputBytes`, `outputBytes`, `shuffleWriteBytes`, `shuffleReadBytes`, `shuffleRemoteBytesRead`, `shuffleFetchWaitTime`, `jvmGcTime`, spill… |

Bỏ qua stage `SKIPPED` (Spark tái dùng kết quả cũ, không chạy).

**`sar`** (gói sysstat) trên mỗi node, mỗi giây một mẫu; `sadf -d` xuất ra CSV:
- `-u`: %CPU (user + nice + system), %iowait;
- `-b`: đọc/ghi đĩa (đơn vị khối 512 byte/giây → nhân 512);
- `-n DEV`: mạng nhận/gửi (kB/giây → nhân 1024; bỏ card `lo`).

Tổng hợp: %CPU lấy trung bình qua các node; byte đĩa và mạng cộng qua các node.

### 5.7 `analysis/report.py`: đồ thị và bảng

Tách dữ liệu:

```python
e1 = runs[runs["shuffle"] == "off"]     # E1: các codec lưu trữ
e2 = runs[runs["storage"] == "none"]    # E2: các codec shuffle
# E3: 4 cặp C0..C3 lấy từ `combined` trong config.yaml
```

| Đồ thị | Nội dung |
|---|---|
| `fig1_time_s_*` | thời gian chạy theo kích thước (mỗi app một ô, mỗi codec một đường, error bar = độ lệch chuẩn qua 5 lần) |
| `fig2_cpu_s_*` | tổng thời gian CPU của executor |
| `fig3_disk_gb_*` | byte đọc + ghi đĩa (sar, mọi node) |
| `fig4_net_gb_*` | byte mạng nhận (sar, mọi node) |
| `fig5_compression_ratio` | tỉ lệ nén lưu trữ (từ `hdfs dfs -du`) và tỉ lệ nén shuffle (byte shuffle tắt nén / có nén) |
| `fig5b_codec_microbench` | micro-benchmark: tỉ lệ nén theo tốc độ nén |
| `fig6_configurations` | thời gian C0–C3 ở kích thước lớn nhất |
| `fig7_workload` | speedup (%) so với không nén, theo từng app |

Công thức:
- **Speedup (%)** = (thời gian không nén − thời gian có nén) / thời gian không nén × 100. Dương = nén làm nhanh hơn, âm = chậm hơn.
- **Tỉ lệ nén lưu trữ** = kích thước bản `none` / kích thước bản codec.
- **Tỉ lệ nén shuffle** = byte shuffle ghi khi tắt nén / khi bật nén.

Đồ thị chính chỉ vẽ **không nén + LZ4 + Zstd** cho dễ đọc (một codec nhanh, một codec nén mạnh, có ở cả hai mức); thư mục `all/` có đủ 4 codec.

### 5.8 Hạ tầng

**Cluster local (`dev/`)**: mô phỏng các máy EC2 bằng container.
- `Dockerfile`: `FROM ubuntu:22.04` (như TP container của môn), `apt-get install` Java 11, SSH, sysstat, PyYAML; tải và giải nén Hadoop 3.4.1, Spark 3.5.5; khoá SSH chung cho mọi container (SSH không mật khẩu); copy `conf/` và `dev/workers`. Daemon chạy bằng `root` trong container (trên EC2 là `ubuntu`).
- `compose.yml`: 3 container (master, worker1, worker2), mỗi container một volume cho dữ liệu HDFS, thư mục project gắn vào `/opt/work`.
- `entrypoint.sh`: mọi container bật SSH server; master chờ SSH của các worker, format NameNode lần đầu, rồi `start-dfs.sh`, `start-master.sh`, `start-workers.sh`, `start-history-server.sh`.

**Terraform (`infra/terraform/`)**: tạo hạ tầng trên AWS bằng code.
- `data "aws_ami"`: tìm image Ubuntu 22.04 x86_64 mới nhất của Canonical.
- `aws_vpc`, `aws_subnet`, `aws_internet_gateway`, `aws_route_table`: mạng riêng 10.0.0.0/16 với một subnet công khai ở một availability zone, để các máy có IP công khai (SSH, tải gói) và nói chuyện với nhau qua IP riêng.
- `aws_security_group`: SSH (cổng 22) chỉ từ IP của bạn; mọi cổng giữa các máy trong cluster; ra Internet thoải mái (tải gói).
- `aws_key_pair`: khoá SSH công khai của bạn.
- `aws_instance` × 4 (`for_each` trên master, worker1..N): cùng loại máy, cùng availability zone, đĩa gp3.
- `local_file`: ghi `inventory.ini` cho Ansible (IP public để kết nối, IP private cho `/etc/hosts`).
- `variables.tf`: vùng, loại máy, số worker, dung lượng đĩa.

**Ansible (`infra/ansible/cluster.yml`)**: cài đặt, theo đúng trình tự của TP:
1. Chờ các máy nhận SSH.
2. Mọi máy: đặt hostname, `/etc/hosts` (IP private của mọi node), tắt cập nhật tự động (tránh nhiễu khi đo), cài gói, tải và giải nén Hadoop + Spark, biến môi trường, **copy file cấu hình `conf/`**, file `workers`, thư mục dữ liệu, kiểm tra `hadoop checknative` báo `zstd: true`.
3. SSH không mật khẩu từ master đến mọi máy (`start-dfs.sh`, `start-workers.sh` và script thí nghiệm cần).
4. Trên master: format NameNode (lần đầu), `start-dfs.sh`, chờ HDFS ra khỏi safe mode, tạo thư mục HDFS, `start-master.sh` + `start-workers.sh` + `start-history-server.sh`, copy code, build jar.

**File cấu hình (`conf/`, dùng chung local và EC2)**: chỉ những gì cần để chạy được, mọi thứ khác là mặc định của Hadoop/Spark (như slide: "not much things to edit in each file").

| File | Nội dung | Vì sao cần |
|---|---|---|
| `hadoop/core-site.xml` | `fs.defaultFS = hdfs://master:9000` | mọi máy biết NameNode ở đâu |
| `hadoop/hdfs-site.xml` | thư mục dữ liệu `/data/hdfs/...`; tắt kiểm tra tên máy ngược (cho container) | mặc định là `/tmp`, bị xoá khi khởi động lại |
| `hadoop/hadoop-env.sh` | `JAVA_HOME` | Hadoop bắt buộc (slide: "JAVA_HOME defined") |
| `spark/spark-env.sh` | `JAVA_HOME`, `HADOOP_CONF_DIR`, `SPARK_MASTER_HOST=master` | Java, tìm cấu hình HDFS, địa chỉ master cố định |
| `spark/spark-defaults.conf` | `spark.master`, event log, thư viện native | app tìm master; History Server có số đo; zstd chạy được |
| `workers` (Ansible ghi / `dev/workers`) | danh sách worker | `start-dfs.sh`, `start-workers.sh` (slide: file "slaves") |

**Tài nguyên Spark: mặc định.** Mỗi worker cho Spark dùng **tất cả core** và **RAM − 1 GB**; mỗi app có **một executor mỗi worker** dùng hết core của worker, **1 GB heap**; driver 1 GB; `defaultParallelism` = **tổng số core** của các executor (ví dụ 3 worker × 2 core = 6). Mặc định này giống nhau ở mọi lần chạy, nên so sánh giữa các codec vẫn công bằng.

**Makefile**: mọi lệnh. Với `ENV=aws`, `make run/status/fetch/shell` SSH vào master (IP lấy từ `terraform output`); `make up/down` gọi Terraform với IP hiện tại của laptop (`curl https://checkip.amazonaws.com`). `make analysis` tự tạo `.venv` (pandas, matplotlib) lần đầu.

---

## 6. Thiết kế thí nghiệm

### 6.1 Ma trận

| | Lưu trữ | Shuffle | Trả lời |
|---|---|---|---|
| E1 | none, gzip, snappy, lz4, zstd | off | nén lưu trữ có lợi không (§5.1) |
| E2 | none | off, lz4, snappy, zstd | nén truyền thông có lợi không (§5.2) |
| E3 | C0 none/off · C1 zstd/off · C2 none/lz4 · C3 zstd/lz4 | | kết hợp hai mức (§5.3) |

× 3 app × SF 1, 5, 10, 20 × 5 lần lặp → 540 lần chạy (mục 5.5).

**Vì sao tách E1 và E2?** Mỗi thí nghiệm chỉ thay đổi **một** yếu tố. Ở E1 shuffle luôn tắt nén, nên mọi khác biệt là do nén lưu trữ. Ở E2 dữ liệu luôn không nén, nên mọi khác biệt là do nén shuffle.

### 6.2 Các biện pháp để số đo đáng tin

| Biện pháp | Tránh được |
|---|---|
| 5 lần lặp, error bar (độ lệch chuẩn) | kết luận từ một lần chạy may/rủi |
| Thứ tự xáo trộn (seed cố định) | nhiễu theo thời gian trùng với một codec |
| 1 lần khởi động mỗi app | lần đầu chậm do JVM, cache |
| Xoá page cache mọi node trước mỗi lần | đọc từ RAM thay vì đĩa (che mất lợi ích nén lưu trữ) |
| Cùng số task cho mọi codec (file < 1 block) | lẫn tác động của nén với tác động của song song |
| Cấu hình Spark giống nhau ở mọi lần chạy (mặc định) | lẫn tác động của nén với cấu hình Spark |
| Máy `m5` (CPU ổn định), đĩa gp3, cùng availability zone | hiệu năng thay đổi giữa đầu và cuối đợt đo |
| Tắt cập nhật tự động | tiến trình nền gây nhiễu |
| Dữ liệu tất định | không tái lập được |

### 6.3 Số đo và ý nghĩa

| Cột `runs.csv` | Nguồn | Ý nghĩa |
|---|---|---|
| `duration_ms` | History Server | thời gian của app (dùng cho đồ thị thời gian) |
| `executor_cpu_ns` | History Server | tổng CPU mọi executor (chi phí nén/giải nén nằm ở đây) |
| `executor_run_ms` | History Server | tổng thời gian chạy của task (CPU + chờ) |
| `input_bytes`, `output_bytes` | History Server | byte đọc/ghi HDFS (sau nén) |
| `shuffle_write_bytes`, `shuffle_read_bytes` | History Server | byte shuffle (sau nén) |
| `shuffle_remote_read_bytes` | History Server | phần shuffle đi qua mạng |
| `shuffle_fetch_wait_ms` | History Server | thời gian task chờ tải dữ liệu shuffle (mạng chậm thì cao) |
| `disk_read_bytes`, `disk_write_bytes` | sar | I/O đĩa thật, mọi node |
| `net_rx_bytes`, `net_tx_bytes` | sar | lưu lượng mạng thật, mọi node |
| `cpu_busy_pct`, `cpu_iowait_pct` | sar | %CPU bận, %CPU chờ I/O |

---

## 7. Đọc kết quả và trả lời 7 câu hỏi

Các nhận định dưới đây là **giả thuyết** dựa trên lý thuyết và số đo ở dev. Báo cáo phải xác nhận hoặc bác bỏ bằng số liệu AWS.

1. **Codec nào cân bằng tốt nhất giữa tỉ lệ nén và CPU?** Micro-benchmark ở dev: zstd-1 nén ~3× (gần gzip 3,3×) nhưng nhanh hơn gzip ~12 lần. LZ4/Snappy nhanh nhất nhưng chỉ ~1,9×. Giả thuyết: **Zstd (mức thấp)** cân bằng tốt nhất; gzip tốn CPU. Xem `fig5b`, `fig5`, `fig2`.
2. **Nén có luôn giúp app I/O-bound?** Không nhất thiết. Lợi ích chỉ lớn hơn chi phí khi thời gian I/O tiết kiệm được > thời gian CPU nén thêm. Gzip có thể làm chậm do nén chậm (27 MB/s mỗi core). Xem `fig1` ô I/O-bound, `speedup.csv`.
3. **Ảnh hưởng tới app CPU-bound?** Gần như không lợi: ít I/O, shuffle ≈ 0. Có thể chậm hơn chút vì giải nén input. Xem `fig1`/`fig2` ô CPU-bound.
4. **Lợi ích thay đổi thế nào khi dữ liệu lớn lên?** Dữ liệu lớn → I/O và shuffle chiếm tỉ trọng lớn hơn, dữ liệu không còn nằm gọn trong bộ nhớ (spill) → lợi ích của nén thường **tăng**. Xem độ dốc trong `fig1`, `speedup.csv` theo `sf`.
5. **Nén lưu trữ hay nén mạng lợi hơn?** Phụ thuộc nút thắt: mạng EC2 nhanh (≥ 1,25 Gbps), đĩa gp3 125 MB/s → đĩa có thể là nút thắt hơn mạng. Xem `fig7` (hai ô), `fig3` với `fig4`, `shuffle_fetch_wait_ms`.
6. **Kết hợp hai mức có thể làm chậm không?** Có thể: C3 cộng cả chi phí CPU của hai mức; nếu một mức không có lợi (ví dụ shuffle nhỏ) thì chỉ còn chi phí. Xem `fig6`.
7. **Khi nào nên tắt nén?** Khi app CPU-bound, khi dữ liệu đã nén sẵn / không nén được (micro-benchmark trên dữ liệu ngẫu nhiên: gzip vẫn tốn CPU), khi mạng và đĩa rất nhanh so với CPU, và khi dùng codec chậm (gzip) cho dữ liệu đọc nhiều lần.

---

## 8. Giới hạn (nên tự nêu khi bảo vệ)

- **Mạng EC2 nhanh**: lợi ích của nén shuffle có thể nhỏ hơn so với cluster có mạng chậm (1 Gbps). Không giả lập mạng chậm vì đề không yêu cầu.
- **File text**: các app đọc cả dòng; với định dạng cột (Parquet) kết quả sẽ khác (đề cho phép, nhưng Parquet cần Spark SQL không có trong môn).
- **File nén không chia nhỏ được**: project cố tình giữ file < 1 block; trong thực tế file lớn nén bằng gzip sẽ mất song song khi đọc.
- **`sortByKey` đọc input hai lần** (job lấy mẫu).
- **zstd lưu trữ** phụ thuộc thư viện native của Hadoop (chỉ x86_64); Spark dùng client Hadoop 3.3.4 với `libhadoop.so` 3.4.1, nếu không tương thích thì bỏ zstd lưu trữ (đề ghi *"where supported"*).
- **Cluster test** (`m7i-flex.large`): CPU "flex" không đảm bảo 100%, số đo không dùng cho báo cáo.
- **Một loại máy, một cấu hình Spark**: kết luận đúng cho cấu hình này; máy khác (nhiều core hơn, đĩa nhanh hơn) có thể cho kết luận khác.

---

## 9. Chuẩn bị demo khi bảo vệ

| Muốn cho thầy xem | Lệnh / file |
|---|---|
| Dựng cluster local | `make up`, mở http://localhost:8080 (3 node) và http://localhost:9870 |
| Chạy toàn bộ trên local | `make run` (khoảng 10–15 phút với SF 0.1) |
| Dữ liệu trên HDFS | `make shell` → `hdfs dfs -ls hdfs://master:9000/data/sf0.1/gzip/lineitem` |
| Một app chạy tay | `spark-submit --class compression.apps.IoBound java/build/cloud-bigdata-project.jar hdfs://master:9000/data/sf0.1/lz4/lineitem hdfs://master:9000/results/demo lz4` |
| Số đo của một lần chạy | History Server http://localhost:18080 → app → Stages (Shuffle Write, Input) |
| Kết quả, đồ thị | `results/aws/runs.csv`, `analysis/figures/aws/` |
| Code hạ tầng | `infra/terraform/main.tf`, `infra/ansible/cluster.yml`, `conf/`, `dev/` |
