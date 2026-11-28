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
| §4 chọn thuật toán + không nén; đo riêng tỉ lệ nén, thời gian nén/giải nén, CPU | chọn **LZ4 và Zstd** (lý do: mục 7), đo riêng bằng `bench/CodecBench.java` |
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
  key pair, máy `master` và 7 máy `worker` (`count = 7`), rồi ghi 2 file cho Ansible:
  `infra/ansible/inventory.ini` (IP của từng máy) và `infra/ansible/workers` (tên các worker).
- Security group: cổng 22 chỉ mở cho **IP hiện tại của laptop**; giữa các máy trong cụm thì mở hết (`self = true`).
- `variables.tf`: loại máy `m7i-flex.large` (2 vCPU, 8 GB; Free plan không cho `m5`), 7 worker
  (quota 16 vCPU = 8 máy), đĩa gp3 100 GB.

### Ansible (`infra/ansible/cluster.yml`)
1. Chờ máy nhận SSH.
2. Trên mọi máy: đặt tên máy, ghi `/etc/hosts` (IP nội bộ + tên), tắt tự cập nhật Ubuntu
   (để không làm nhiễu số đo), chạy `cluster/install.sh`, copy file `workers`.
3. Trên master: copy khoá SSH của laptop sang (để master SSH được tới mọi máy), chạy
   `cluster/start-cluster.sh` (bật HDFS, Spark, History Server), copy code.

### SSH không mật khẩu: một cặp khoá
- `make up` tạo cặp khoá `~/.ssh/cloud-bigdata-project` trên laptop (`ssh-keygen`). Terraform gửi **khoá công
  khai** lên AWS, AWS ghi nó vào `~/.ssh/authorized_keys` của cả 8 máy: laptop SSH được vào mọi máy
  (Ansible, `make run/shell/fetch`).
- Ansible copy **khoá riêng** đó sang master. Vì khoá công khai đã có trên cả 8 máy, master cũng SSH được tới
  mọi máy mà không cần mật khẩu (`start-dfs.sh`, `start-workers.sh`, `run_experiments.py` xoá cache, bật `sar`).
- Nguyên lý: khoá riêng nằm ở máy đi SSH, khoá công khai nằm trong `authorized_keys` của máy được SSH vào.
  Khoá riêng nằm trên master chỉ trong lúc cụm chạy; `make down` xoá máy là xoá luôn.

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
- **150.000 đơn mỗi file** (file lineitem ≈ 75 MB < 1 block HDFS 128 MB): file text nén bằng lz4/zstd
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
| Lưu trữ | ghi file bằng codec Hadoop: `rdd.saveAsTextFile(path, ZStandardCodec.class)`. Khi đọc, `textFile()` tự nhận codec theo đuôi file (`.lz4`, `.zst`) | `Util.save()`; codec là tham số dòng lệnh của app |
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
một file lineitem 75 MB trong RAM, trên 1 luồng (nên thời gian = thời gian CPU), lặp 5 lần (bỏ 2 lần khởi động).
- Chỉ đo **2 codec của nghiên cứu**: LZ4 và Zstd, bằng **đúng các class Spark dùng cho shuffle**.
- Zstd đo ở 2 mức vì cụm dùng cả hai: level 1 (mặc định của Spark, cho shuffle), level 3 (mặc định của
  Hadoop, cho file).
- Chạy trên master trước các thí nghiệm (master rảnh, cùng loại máy với worker).
- Kiểm chứng: tỉ lệ nén đo trên HDFS và trên shuffle (hình 5 trái của báo cáo) gần bằng bench.

| Codec | Tỉ lệ nén | Thời gian nén | Thời gian giải nén | Tốc độ nén |
|---|---|---|---|---|
| LZ4 | 1,91 | 0,24 s | 0,10 s | 316 MB/s |
| Zstd level 1 | 3,15 | 0,30 s | 0,10 s | 252 MB/s |
| Zstd level 3 | 3,22 | 0,42 s | 0,12 s | 181 MB/s |

### Vì sao chọn 2 codec LZ4 và Zstd

- Đề cho 4 codec (Snappy, LZ4, Gzip, Zstd); nhóm chọn 2.
- LZ4 và Zstd là **hai đầu của sự đánh đổi**: LZ4 nhanh nhưng nén ít (~2×), Zstd nén nhiều (~3×) nhưng tốn CPU
  hơn. Snappy gần giống LZ4; Gzip nén gần bằng Zstd nhưng chậm hơn nhiều và **Spark không cho dùng Gzip để
  nén shuffle**.
- Cả LZ4 và Zstd đều dùng được **ở cả hai mức** (file và shuffle), nên mọi thí nghiệm E1, E2, E3 so sánh
  cùng một cặp.

## 8. Thí nghiệm (`bench/run_experiments.py`)

6 cặp (codec lưu trữ, codec shuffle): E1 có 3 (none/off, lz4/off, zstd/off), E2 thêm 2 (none/lz4, none/zstd),
E3 thêm C3 = zstd/lz4 (C0, C1, C2 đã có trong E1/E2).
6 cặp × 3 app × 4 kích thước × 3 lần = **216 lần chạy**.

Mỗi lần chạy:
1. Trên mọi máy (qua SSH): xoá page cache (để dữ liệu thật sự được đọc từ đĩa) và bật `sar` mỗi giây.
2. `spark-submit` app với các `--conf` nén.
3. Tắt `sar`, lấy số liệu (`sadf -d`), lưu bản thô vào `results/sar/<run>/` (trong repo: nén thành `results/sar.tar.gz`, giải nén bằng `tar xzf results/sar.tar.gz -C results`).
4. Hỏi History Server số đo của app, cộng số đo `sar`, ghi **một dòng** vào `results/runs.csv`.

Để công bằng: thứ tự 216 lần chạy được **xáo trộn** (seed cố định), mỗi app chạy khởi động 1 lần không tính,
mọi lần chạy dùng cùng cụm và cùng cấu hình. Bị ngắt thì chạy lại `make run`: lần đã `ok` được bỏ qua.

Các cột của `runs.csv` (chỉ giữ những số đo đề yêu cầu):

| Cột | Nguồn | Ý nghĩa | Đề yêu cầu |
|---|---|---|---|
| `time_s` | History Server | thời gian chạy app | §5.1, §5.2 |
| `executor_run_ms`, `executor_cpu_ns` | History Server | thời gian chạy và thời gian CPU của mọi task (chi phí nén nằm trong CPU) | §3, §5.1, §5.2 |
| `shuffle_write_bytes` | History Server | byte shuffle (đã nén) | §5.2 |
| `shuffle_write_ns`, `shuffle_fetch_wait_ms` | History Server | thời gian shuffle = ghi + chờ kéo dữ liệu | §5.2 |
| `cpu_busy_pct`, `cpu_iowait_pct`, `mem_used_pct` | `sar` | CPU, I/O wait, RAM: trung bình trên 8 máy | §2, §3, §5.1 |
| `disk_read_bytes`, `disk_write_bytes`, `net_rx_bytes` | `sar` | đĩa đọc + ghi, mạng nhận: tổng trên 8 máy | §5.1, §5.2 |

Thông lượng đĩa và mạng (§5.1, §5.2) = lượng dữ liệu / thời gian, tính trong `analysis/report.py`.

## 9. Đồ thị và 7 câu hỏi của đề

Báo cáo tối đa 5 trang, nên 7 đồ thị bắt buộc được gom vào 4 hình (hình 2–5 của báo cáo):

| Hình (báo cáo) | File | Đồ thị đề yêu cầu (§6) |
|---|---|---|
| 2 | `fig1_time_vs_size` | thời gian theo kích thước, mỗi codec một đường (hàng 1: E1, hàng 2: E2) |
| 3 | `fig2_cpu_vs_size` | CPU theo kích thước: chi phí nén |
| 4 | `fig3_disk_network_vs_size` | hàng 1: đĩa theo kích thước (nén lưu trữ); hàng 2: mạng theo kích thước (nén shuffle) |
| 5 | `fig4_ratio_configurations_workload` | 3 ô: tỉ lệ nén theo thuật toán; thời gian C0–C3; thời gian theo loại workload |

| Câu hỏi | Trả lời bằng |
|---|---|
| 1. Codec nào cân bằng tốt nhất giữa tỉ lệ nén và CPU? | bảng 2 (bench) + mục 6 (so LZ4 và Zstd trên cụm) |
| 2. Nén có luôn giúp app I/O-bound? | bảng 3, hình 5 phải |
| 3. Nén ảnh hưởng app CPU-bound thế nào? | bảng 3, hình 5 phải |
| 4. Lợi ích thay đổi thế nào khi dữ liệu lớn dần? | bảng 3, hình 2 |
| 5. Nén lưu trữ hay nén mạng lợi hơn? | hình 5 phải, đoạn E1 và E2 của mục 6 |
| 6. Nén cả hai có làm chậm đi không? | hình 5 giữa: C3 so với C1, C2 |
| 7. Khi nào nên tắt nén? | dữ liệu nhỏ (SF 1), app CPU-bound, shuffle rất nhỏ, CPU đã là nút thắt |

Đề nhấn mạnh: kết luận phải dựa trên **số đo**, không phải lý thuyết.

## 10. Những điểm nên biết khi bảo vệ (không viết trong báo cáo)

- **Máy "flex"**: CPU không bị giới hạn bằng credit như t3, nhưng đĩa và mạng có burst (mức nền 39 MB/s và
  0,39 Gbps). Đĩa worker vẫn đạt ~110 MB/s khi chạy app I/O-bound: I/O chưa bão hoà, nên lợi ích của nén nhỏ
  hơn so với một cụm nghẽn I/O.
- **File text nén không chia nhỏ được** (lz4, zstd): ở đây vô hại vì mọi file < 1 block.
- **zstd khác level**: shuffle level 1, file level 3.
- **HDFS lưu 3 bản sao**: ghi output cũng sinh lưu lượng mạng, nên mạng đo bằng `sar` gồm cả HDFS, không chỉ shuffle.
- `sortByKey` đọc input **hai lần** (một lần để lấy mẫu chia khoảng key).
- Chỉ 3 lần lặp mỗi cấu hình (giới hạn thời gian và chi phí); error bar cho thấy độ dao động nhỏ.

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
- **Vì sao chọn LZ4 và Zstd?** Hai đầu của sự đánh đổi (nhanh/nén ít và nén nhiều/tốn CPU hơn), cả hai dùng
  được cho file lẫn shuffle (Spark không có Gzip cho shuffle). Xem mục 7.
- **Vì sao 150.000 đơn mỗi file?** Để file < 128 MB: mọi codec có cùng số task.
- **Vì sao executor 3 GB?** Mặc định 1 GB làm dữ liệu tràn ra đĩa ở SF lớn, thời gian đo sẽ chủ yếu là thời gian tràn.
- **Nếu IP nhà đổi?** `make up` lại: Terraform cập nhật tường lửa theo IP mới.

## 12. Chi phí và an toàn

- Khi cụm bật: ≈ 1,1 USD/giờ (8 máy ≈ 0,96 + đĩa ≈ 0,10 + IP ≈ 0,04). Đợt chính thức (216 lần chạy) ≈ 14–17 giờ ≈ 17–22 USD.
- Cụm tính tiền **kể cả khi không chạy gì**: xong là `make fetch`, kiểm tra `results/runs.csv`, rồi `make down`,
  rồi kiểm tra trên console EC2 (vùng Sydney) không còn máy hay ổ đĩa nào.
- Trong lúc chạy, `make fetch` định kỳ để không mất kết quả nếu tài khoản bị dừng.
