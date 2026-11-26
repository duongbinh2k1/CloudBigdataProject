# Giải thích project: nén dữ liệu trong Spark

Tài liệu duy nhất để hiểu, chạy và bảo vệ project. Đề bài: `../CoursCloudN7/ProjectUSTH.pdf`.
Cách chạy ngắn gọn: [README](../README.md).

## 1. Đề hỏi gì, project trả lời ở đâu

Câu hỏi chính: **nén dữ liệu lúc nào làm Spark chạy nhanh hơn, lúc nào làm chậm đi?**
Nén thì ít byte phải đọc/ghi/gửi hơn, nhưng tốn CPU để nén và giải nén. Ta đo để biết bên nào thắng.

| Đề yêu cầu | Trong project |
|---|---|
| §2 HDFS phân tán, nhiều worker, giám sát CPU/RAM/đĩa/mạng, tái lập được | 8 máy EC2 (1 master + 7 worker) tạo bằng Terraform, cài bằng Ansible. Đo bằng `sar` trên mọi máy + Spark History Server |
| §3 Ba app I/O-bound, CPU-bound, balanced, chứng minh bằng số đo | `IoBound`, `CpuBound`, `Balanced`. Bảng `analysis/tables/workload_characterisation.csv` |
| §4 Snappy, LZ4, Gzip, Zstd + không nén; đo riêng tỉ lệ nén, tốc độ, CPU | cả 4 codec đo riêng bằng `bench/CodecBench.java` → hình 5, 8. Trên cluster chỉ dùng **LZ4 và Zstd** (lý do: mục 7) |
| §5.1 Nén lưu trữ | E1: file trên HDFS không nén / LZ4 / Zstd, shuffle không nén |
| §5.2 Nén truyền thông (shuffle) | E2: shuffle không nén / LZ4 / Zstd, file không nén |
| §5.3 C0–C3 | E3: C0 none/off, C1 zstd/off, C2 none/lz4, C3 zstd/lz4 |
| §5.4 Nhiều kích thước | SF 1, 5, 10, 20 (≈ 1, 5, 10, 20 GB) |
| §6 Đồ thị, lặp nhiều lần, error bar | `analysis/report.py`: 8 hình, mỗi cấu hình chạy 3 lần, error bar = độ lệch chuẩn |
| §7 Nộp hạ tầng, số liệu thô, script, báo cáo | `infra/`, `cluster/`, `results/` (runs.csv + sar thô), `bench/`, `analysis/` |

## 2. Toàn cảnh: chạy một lệnh thì chuyện gì xảy ra

```
laptop                                   AWS (Sydney, cùng một zone)
──────                                   ───────────────────────────────────────────
make up ── Terraform ──► tạo 8 máy ──►  master : NameNode, Spark Master, History Server, driver
        └─ Ansible ────► cài + khởi động  worker1..7 : DataNode + Spark Worker
make run ── ssh ───────► master chạy bench/run_all.sh trong tmux:
                           1. build jar   2. sinh dữ liệu   3. CodecBench   4. 216 lần chạy
make fetch ◄── rsync ─── results/
make analysis            → analysis/figures/*.png, analysis/tables/*.csv
make down  ── Terraform ─► xoá hết
```

Công nghệ dùng, mỗi cái một việc:

| Công nghệ | Việc |
|---|---|
| Terraform | tạo máy, mạng, tường lửa trên AWS (Infrastructure as Code, TP Terraform) |
| Ansible | cài đặt và khởi động trên các máy qua SSH (TP Ansible) |
| HDFS (Hadoop) | lưu dữ liệu phân tán |
| Spark standalone | chạy 3 app (Java, API RDD như slide môn học) |
| `sar` | đo CPU, RAM, đĩa, mạng mỗi giây trên từng máy |
| Spark History Server | số đo của từng app: thời gian CPU, byte shuffle, thời gian shuffle |
| Python | điều phối thí nghiệm, vẽ đồ thị |
| Make | gom các lệnh: `make up`, `make run`… |

## 3. Hạ tầng

### Terraform (`infra/terraform/`)
- `main.tf`: VPC + subnet + internet gateway (tài khoản không có mạng mặc định), security group,
  key pair, 8 máy, và file `infra/ansible/inventory.ini` (IP của từng máy cho Ansible).
- Security group: cổng 22 chỉ mở cho **IP hiện tại của laptop**; giữa các máy trong cụm thì mở hết (`self = true`).
- `variables.tf`: loại máy `m7i-flex.large` (2 vCPU, 8 GB; Free plan không cho `m5`), 7 worker
  (quota 16 vCPU = 8 máy), đĩa gp3 100 GB.

### Ansible (`infra/ansible/cluster.yml`)
1. Chờ máy nhận SSH.
2. Trên mọi máy: đặt tên máy, ghi `/etc/hosts` (IP nội bộ + tên), tắt tự cập nhật Ubuntu
   (để không làm nhiễu số đo), chạy `cluster/install.sh`, ghi file `workers`.
3. SSH không mật khẩu từ master tới mọi máy.
4. Trên master: `cluster/start-cluster.sh` (bật HDFS, Spark, History Server), copy code.

### SSH không mật khẩu: hai cặp khoá
| Chặng | Khoá tạo bởi | Dùng cho |
|---|---|---|
| laptop → 8 máy | Makefile (`ssh-keygen`), Terraform gửi khoá công khai lên AWS | Ansible, `make run/shell/fetch` |
| master → 8 máy | Ansible (`ssh-keygen` trên master, đọc khoá công khai, thêm vào `authorized_keys` mọi máy) | `start-dfs.sh`, `start-workers.sh`, `run_experiments.py` (xoá cache, bật `sar`) |

Nguyên lý: khoá riêng nằm ở máy đi SSH, khoá công khai nằm trong `~/.ssh/authorized_keys` của máy được SSH vào.

### Master tìm worker thế nào
- Danh sách tên: file `workers` (`worker1`…`worker7`), Ansible ghi từ inventory.
- Tên → IP: `/etc/hosts`, Ansible ghi từ IP nội bộ mà Terraform trả về.
- `start-dfs.sh` / `start-workers.sh` SSH theo tên để **bật** DataNode / Worker. Sau đó các tiến trình
  trên worker **tự kết nối về** master: `hdfs://master:9000` (`core-site.xml`) và
  `spark://master:7077` (`spark-defaults.conf`). Shuffle đi thẳng giữa các worker, không qua master.

### Cấu hình Hadoop/Spark (`cluster/`)
Gần như mặc định, chỉ thêm những dòng cần thiết: địa chỉ NameNode và Spark Master, thư mục dữ liệu HDFS,
event log cho History Server, thư viện native cho zstd, và `spark.executor.memory 3g`
(mặc định 1 GB thì ở SF 20 dữ liệu tràn ra đĩa liên tục, số đo bị méo).

## 4. Dữ liệu (`DataGen.java`)

- Hai bảng của benchmark **TPC-H**: `orders` (1,5 triệu dòng × SF) và `lineitem` (1–7 dòng mỗi đơn,
  ≈ 6 triệu dòng × SF), dạng text, cột cách nhau bằng `|`. SF 1 ≈ 0,93 GB.
- Vì sao TPC-H: giống dữ liệu doanh nghiệp (số, ngày, chữ lặp lại) nên **nén được 2–3 lần** như dữ liệu thật;
  có khoá chung để `join`.
- Mỗi đơn hàng có bộ sinh ngẫu nhiên riêng `SplittableRandom(mã đơn)`: chạy lại hay đổi số máy vẫn ra
  **đúng dữ liệu cũ**, và 3 bản (none, lz4, zstd) có nội dung giống hệt nhau.
- **150.000 đơn mỗi file** (file lineitem ≈ 75 MB < 1 block HDFS 128 MB): file text nén bằng lz4/zstd/gzip
  không chia nhỏ được, nên giữ file nhỏ để mọi codec có **cùng số task** (1 file = 1 task).
- Mỗi kích thước được ghi 3 bản (none, lz4, zstd) vào `/data/sf<N>/<codec>/`.
  Tổng ≈ 62 GB, × 3 bản sao HDFS ≈ 27 GB mỗi worker.

## 5. Ba ứng dụng (`java/src/compression/`)

| App | Làm gì | Vì sao thuộc loại đó |
|---|---|---|
| `IoBound` | `sortByKey` toàn bộ `lineitem` theo ngày giao, ghi lại | đọc, shuffle, ghi **toàn bộ** dữ liệu; gần như không tính toán |
| `CpuBound` | SHA-256 lặp 64 lần mỗi dòng, lấy max theo 7 hình thức giao | `reduceByKey` gộp trước ở phía map → shuffle ≈ 0; thời gian là CPU |
| `Balanced` | `join` lineitem với orders, doanh thu + SHA-256 lặp 8 lần, tổng theo khách hàng | shuffle lớn (cả 2 bảng) **và** có tính toán |

Số partition để mặc định: bằng số file đầu vào (10 × SF), tức mỗi task khoảng 75 MB.
Chứng minh loại app bằng bảng `workload_characterisation.csv` (chạy không nén): `cpu_share`
(thời gian CPU / thời gian task), `mb_moved_per_cpu_s` (MB dữ liệu di chuyển trên mỗi giây CPU),
`cpu_iowait_pct`, byte đĩa và mạng.

## 6. Bật/tắt nén: hai công tắc

| Mức | Cách bật | Ở đâu |
|---|---|---|
| Lưu trữ | ghi file bằng codec Hadoop: `rdd.saveAsTextFile(path, ZStandardCodec.class)`. Khi đọc, `textFile()` tự nhận codec theo đuôi file (`.gz .snappy .lz4 .zst`) | `Util.save()`; codec là tham số dòng lệnh của app |
| Shuffle | chỉ cấu hình, không sửa code: `--conf spark.shuffle.compress=true --conf spark.io.compression.codec=lz4` | `run_experiments.py`, hàm `spark_submit()` |

- Không phải tự viết thuật toán nén: Spark và Hadoop có sẵn.
- `spark.shuffle.spill.compress` bật/tắt cùng shuffle (dữ liệu tràn ra đĩa cũng là dữ liệu trung gian).
- zstd: Spark dùng level 1 cho shuffle, Hadoop dùng level 3 cho file. zstd cho file cần thư viện native
  (`libzstd1` + `libhadoop.so`).

### Shuffle là gì (dữ liệu trung gian)
Giữa hai stage (ví dụ trước và sau `sortByKey`, `join`, `reduceByKey`): map task ghi dữ liệu đã chia theo
key ra **đĩa local**, reduce task kéo phần của mình từ mọi máy **qua mạng**. Nén ở bước ghi nên tiết kiệm
cả đĩa local lẫn mạng; cái giá là CPU nén (map) và giải nén (reduce).

## 7. Đo riêng codec (`bench/CodecBench.java`)

§4 bắt đo **đặc tính riêng** của thuật toán: không Spark job, không HDFS, không mạng. Nén rồi giải nén
một file lineitem 75 MB trong RAM, trên 1 luồng (nên thời gian = thời gian CPU), lặp 5 lần.
- lz4, snappy, zstd: **đúng các class Spark dùng cho shuffle**, tham số mặc định; zstd đo cả level 1 và 3.
- gzip: `java.util.zip` level 6 (= mặc định của `GzipCodec` Hadoop).
- Thêm 64 MB byte ngẫu nhiên (không nén được): trường hợp nén chỉ tốn CPU mà không được gì (câu hỏi 7).
- Chạy trên master trước các thí nghiệm (master rảnh, cùng loại máy với worker).
- Kiểm chứng: tỉ lệ nén đo trên HDFS (hình 5 giữa) gần bằng bench (hình 5 trái).

### Vì sao trên cluster chỉ chọn 2 codec: LZ4 và Zstd

Số đo bench của bản trước (file lineitem 75 MB, 1 luồng, trung vị 5 lần):

| Codec | Tỉ lệ nén | Nén (MB/s) | Giải nén (MB/s) | Có cho file | Có cho shuffle |
|---|---|---|---|---|---|
| LZ4 | 1,87 | 360 | 1237 | có | có (mặc định của Spark) |
| Snappy | 1,96 | 502 | 866 | có | có |
| Zstd level 1 (shuffle) | 3,02 | 324 | 762 | | có |
| Zstd level 3 (file) | 3,14 | 186 | 629 | có | |
| Gzip level 6 | 3,29 | 27 | 321 | có | **không** |

Bốn codec thực ra chỉ có **hai nhóm**:
- nhóm "nhanh, nén ít" (~1,9×): LZ4 và Snappy, gần như giống nhau, nên chỉ cần một đại diện;
- nhóm "nén mạnh" (~3,1–3,3×): Zstd và Gzip.

Chọn một đại diện mỗi nhóm thì hai codec **lệch nhau nhiều nhất về tỉ lệ nén** (Zstd ít hơn LZ4 khoảng 1,7 lần
số byte), nên khác biệt thấy rõ trên đồ thị đĩa và mạng. Trong nhóm "nén mạnh", chọn **Zstd chứ không Gzip**
vì Spark không cho Gzip nén shuffle: với Zstd, **cùng hai codec có mặt ở cả E1, E2, E3**, mọi đồ thị so sánh
cùng một cặp, dễ đọc và dễ giải thích. Gzip (chậm hơn LZ4 khoảng 13 lần) và Snappy vẫn có trong bench, nên
câu hỏi 1 (codec nào cân bằng tốt nhất) vẫn trả lời được với đủ 4 codec.

Lưu ý khi bảo vệ: ở mức shuffle (level 1) Zstd tốn CPU gần như LZ4, nên khác biệt chủ yếu là **tỉ lệ nén**;
ở mức file (level 3) Zstd nén chậm hơn LZ4 khoảng 2 lần. Bỏ Snappy và Gzip khỏi cluster cũng giảm số lần chạy
từ 324 xuống 216 (tiết kiệm khoảng 7 giờ và 8 USD).

## 8. Thí nghiệm (`bench/experiments.yaml`, `run_experiments.py`)

6 cặp (codec lưu trữ, codec shuffle): E1 có 3 (none/off, lz4/off, zstd/off), E2 thêm 2 (none/lz4, none/zstd),
E3 thêm C3 = zstd/lz4 (C0, C1, C2 đã có trong E1/E2).
6 cặp × 3 app × 4 kích thước × 3 lần = **216 lần chạy**.

Mỗi lần chạy:
1. Trên mọi máy (qua SSH): xoá page cache (để dữ liệu thật sự được đọc từ đĩa) và bật `sar` mỗi giây.
2. `spark-submit` app với các `--conf` nén.
3. Tắt `sar`, lấy số liệu (`sadf -d`), lưu bản thô vào `results/sar/<run>/`.
4. Hỏi History Server số đo của app, cộng số đo `sar`, ghi **một dòng** vào `results/runs.csv`.

Để công bằng: thứ tự 216 lần chạy được **xáo trộn** (seed cố định), mỗi app chạy khởi động 1 lần không tính,
mọi lần chạy dùng cùng cụm và cùng cấu hình. Bị ngắt thì chạy lại `make run`: lần đã `ok` được bỏ qua.

Các cột chính của `runs.csv`:

| Cột | Nguồn | Ý nghĩa |
|---|---|---|
| `time_s` | History Server | thời gian chạy app |
| `executor_cpu_ns` | History Server | tổng thời gian CPU của mọi task (chi phí nén nằm ở đây) |
| `input_bytes`, `output_bytes` | History Server | byte đọc/ghi HDFS (đã nén) |
| `shuffle_write_bytes`, `shuffle_remote_read_bytes` | History Server | byte shuffle (đã nén), phần đi qua mạng |
| `shuffle_write_ns`, `shuffle_fetch_wait_ms` | History Server | thời gian shuffle = ghi + chờ kéo dữ liệu |
| `spill_bytes` | History Server | dữ liệu tràn ra đĩa vì thiếu RAM |
| `cpu_busy_pct`, `cpu_iowait_pct`, `mem_used_pct` | `sar` | trung bình trên 8 máy |
| `disk_read_bytes`, `disk_write_bytes`, `net_rx_bytes`, `net_tx_bytes` | `sar` | tổng trên 8 máy |

## 9. Đồ thị và 7 câu hỏi của đề

| Hình | Nội dung (đề §6) |
|---|---|
| `fig1_time_vs_size` | thời gian theo kích thước, mỗi codec một đường (hàng 1: E1, hàng 2: E2) |
| `fig2_cpu_vs_size` | CPU theo kích thước: chi phí nén |
| `fig3_disk_vs_size` | đĩa theo kích thước: tác dụng của nén lưu trữ |
| `fig4_network_vs_size` | mạng theo kích thước: tác dụng của nén shuffle |
| `fig5_compression_ratio` | tỉ lệ nén: bench, file HDFS, shuffle |
| `fig6_configurations` | thời gian của C0–C3, SF lớn nhất |
| `fig7_workload_type` | % nhanh hơn so với không nén, theo loại app |
| `fig8_codec_tradeoff` | tỉ lệ nén vs tốc độ nén (bench) |

| Câu hỏi | Trả lời bằng |
|---|---|
| 1. Codec nào cân bằng tốt nhất giữa tỉ lệ nén và CPU? | hình 8 + bảng `codec_bench.csv` (đủ 4 codec), rồi hình 1–2 (LZ4 vs Zstd trên cluster) |
| 2. Nén có luôn giúp app I/O-bound? | hình 1 và 7, cột I/O-bound; so LZ4 với Zstd |
| 3. Nén ảnh hưởng app CPU-bound thế nào? | hình 7 (gần 0%), hình 2 |
| 4. Lợi ích thay đổi thế nào khi dữ liệu lớn dần? | độ dốc các đường trong hình 1 |
| 5. Nén lưu trữ hay nén mạng lợi hơn? | hình 7 trái vs phải, hình 3 vs hình 4 |
| 6. Nén cả hai có làm chậm đi không? | hình 6: C3 so với C1, C2 |
| 7. Khi nào nên tắt nén? | dữ liệu nhỏ, app CPU-bound, dữ liệu không nén được (bench "random": gzip tốn CPU mà không giảm byte nào) |

Đề nhấn mạnh: kết luận phải dựa trên **số đo**, không phải lý thuyết. Viết kiểu
"ở SF 20, lz4 làm `io_bound` nhanh hơn X% (hình 7) vì byte shuffle giảm Y lần (hình 5) trong khi CPU chỉ tăng Z% (hình 2)".

## 10. Giới hạn cần nêu trong báo cáo

- **Máy "flex"**: CPU không bị giới hạn bằng credit như t3, nhưng **đĩa và mạng có burst**: mức nền chỉ
  39 MB/s (đĩa) và 0,39 Gbps (mạng). Bước sinh dữ liệu (~1 giờ) dùng hết credit nên phần lớn các lần đo chạy
  ở mức nền. Cụm vì vậy nghẽn I/O nặng hơn cụm thông thường → nén có thể tỏ ra lợi hơn.
- **File text nén không chia nhỏ được** (lz4, zstd, gzip): ở đây vô hại vì mọi file < 1 block, nhưng với file lớn sẽ mất song song.
- **Chỉ 2 codec trên cluster**: Gzip và Snappy chỉ đo bằng bench (lý do ở mục 7). Gzip không có cho shuffle trong Spark.
- **zstd khác level**: shuffle level 1, file level 3.
- **HDFS lưu 3 bản sao**: ghi output cũng sinh lưu lượng mạng; muốn xem riêng shuffle thì dùng
  `shuffle_remote_read_bytes`.
- `sortByKey` đọc input **hai lần** (một lần để lấy mẫu chia khoảng key).
- Chỉ 3 lần lặp mỗi cấu hình (giới hạn thời gian và chi phí); error bar cho thấy độ dao động.

## 11. Câu hỏi bảo vệ thường gặp

- **Vì sao Spark mà không MapReduce?** Spark có cấu hình nén shuffle riêng, tách khỏi nén file; History
  Server cho sẵn byte shuffle và thời gian CPU. Spark là nội dung chính của môn.
- **Vì sao standalone, không YARN?** Đề không yêu cầu; ít tiến trình hơn, ít nhiễu hơn. Hadoop chỉ dùng cho HDFS.
- **Vì sao ít nhất nhiều máy?** Để shuffle đi qua mạng thật. Trên 1 máy, nén truyền thông vô nghĩa.
- **Vì sao không dùng Docker/Kubernetes trên AWS?** Thêm một lớp mạng ảo vào đúng thứ đang đo (mạng).
- **Vì sao xoá page cache?** Nếu không, lần chạy sau đọc dữ liệu từ RAM, không còn đo I/O đĩa.
- **Vì sao xáo trộn thứ tự?** Để một khoảng thời gian cụm chậm không rơi vào toàn bộ một codec.
- **CodecBench có đo đúng như Spark không?** Dùng đúng class codec của Spark với tham số mặc định; khác ở
  chỗ dữ liệu shuffle thật là bản ghi đã serialize, chia nhiều khối nhỏ. Tỉ lệ nén thật lấy từ HDFS và History Server.
- **Vì sao chỉ LZ4 và Zstd trên cluster?** Bốn codec chia hai nhóm (nhanh/nén ít và chậm hơn/nén mạnh); mỗi
  nhóm lấy một đại diện có mặt ở cả hai mức nén. Bench vẫn đo đủ 4 codec để chứng minh lựa chọn này (mục 7).
- **Vì sao 150.000 đơn mỗi file?** Để file < 128 MB: mọi codec có cùng số task.
- **Vì sao executor 3 GB?** Mặc định 1 GB làm dữ liệu tràn ra đĩa ở SF lớn, thời gian đo sẽ chủ yếu là thời gian tràn.
- **Nếu IP nhà đổi?** `make up` lại: Terraform cập nhật tường lửa theo IP mới.

## 12. Chi phí và an toàn

- Khi cụm bật: ≈ 1,1 USD/giờ (8 máy ≈ 0,96 + đĩa ≈ 0,10 + IP ≈ 0,04). Đợt chính thức (216 lần chạy) ≈ 14–17 giờ ≈ 17–22 USD.
- Cụm tính tiền **kể cả khi không chạy gì**: xong là `make fetch`, kiểm tra `results/runs.csv`, rồi `make down`,
  rồi kiểm tra trên console EC2 (vùng Sydney) không còn máy hay ổ đĩa nào.
- Trong lúc chạy, `make fetch` định kỳ để không mất kết quả nếu tài khoản bị dừng.
