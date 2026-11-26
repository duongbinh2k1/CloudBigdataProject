# 00 — Đề bài: yêu cầu gì, hiểu thế nào, dẫn tới thiết kế ra sao

Đề: *InfraCloud Project: Experimental Evaluation of Data Compression in Hadoop or Spark* (`CoursCloudN7/ProjectUSTH.pdf`). Phần này đi qua **từng mục của đề**, mỗi mục gồm:
- **Đề nói gì**: tóm tắt bằng tiếng Việt;
- **Nghĩa là phải làm gì**: giải thích cụ thể;
- **Project làm thế nào**: lựa chọn thiết kế và file tương ứng.

Cuối file là bảng kiểm tra toàn bộ yêu cầu, gồm cả những chỗ **còn thiếu**.

---

## §1. Bối cảnh và mục tiêu

**Đề nói gì.** Hadoop và Spark xử lý dữ liệu lớn trên nhiều máy. Hiệu năng phụ thuộc CPU, tốc độ đĩa, băng thông mạng và lượng dữ liệu trao đổi giữa các máy. Nén làm dữ liệu nhỏ đi (ít chỗ lưu, ít truyền), nhưng nén/giải nén tốn CPU, có thể làm chậm. Lợi hay hại phụ thuộc vào: **thuật toán nén**, **kiểu ứng dụng**, **hạ tầng**, **lượng dữ liệu**. Nén có thể đặt ở hai chỗ:
- **mức lưu trữ**: nén dữ liệu trước khi lưu vào HDFS (ít chỗ đĩa, có thể đọc/ghi đĩa nhanh hơn);
- **mức truyền thông**: nén dữ liệu trung gian trao đổi giữa các task, nhất là khi **shuffle** (ít lưu lượng mạng).

Mục tiêu: **đo bằng thực nghiệm** tác động lên hiệu năng và chi phí tính toán của các thuật toán nén, ở cả hai mức. Cần một nghiên cứu **chặt chẽ**, có **đường cong hiệu năng**, và chỉ ra **khi nào nén giúp, khi nào nén hại**.

**Nghĩa là phải làm gì.**
- Không phải viết một ứng dụng "có nén", mà là **một nghiên cứu đo đạc**: dựng hệ thống, chạy cùng một việc với nhiều cấu hình nén, đo, so sánh, giải thích.
- "Chặt chẽ" (*rigorous*): thay đổi **một yếu tố mỗi lần**, **lặp lại** nhiều lần, kiểm soát nhiễu, **tái lập** được.
- Kết luận phải đến từ **số đo**, không từ lý thuyết chung.

**Project làm thế nào.** Chọn **Spark** (mục §2). Hai mức nén được tách hẳn thành hai thí nghiệm riêng (E1, E2) cộng một thí nghiệm kết hợp (E3). Mọi lần đo do script tự động chạy và ghi lại (`bench/`), đồ thị và bảng do script vẽ (`analysis/`).

---

## §2. Hạ tầng thí nghiệm

**Đề nói gì.** Dựng và cấu hình môi trường Hadoop/Spark phân tán trên **một cluster máy ảo**, gồm:
1. hệ thống lưu trữ **HDFS** phân tán;
2. môi trường chạy **MapReduce hoặc Spark**;
3. **nhiều worker**;
4. **công cụ giám sát** đo CPU, **bộ nhớ**, I/O đĩa, lưu lượng mạng.

Môi trường phải **tái lập được**, và **cùng một cấu hình hạ tầng** khi so sánh các thuật toán nén.

**Nghĩa là phải làm gì.**
- Cluster thật, nhiều máy (thầy yêu cầu ít nhất 2 máy, không cài hết lên một máy), để shuffle đi qua **mạng thật**.
- Dựng bằng script (không làm tay), để ai cũng dựng lại được y hệt.
- Có công cụ đo tài nguyên trên từng máy.
- Khi so sánh codec, chỉ được đổi codec; mọi thứ khác giữ nguyên.

**Project làm thế nào.**

| Yêu cầu | Lựa chọn | File |
|---|---|---|
| Cluster máy ảo | 8 máy EC2 `m7i-flex.large` (AWS) Ubuntu 22.04: 1 master + 7 worker | `infra/terraform/` |
| HDFS | Hadoop 3.4.1, chỉ dùng HDFS: NameNode trên master, DataNode trên mỗi worker | `cluster/hadoop/` |
| Spark | Spark 3.5.5 **standalone**: Master trên master, Worker trên mỗi worker | `cluster/spark/` |
| Nhiều worker | 7 (tối đa với quota 16 vCPU của tài khoản) | `variables.tf` |
| Giám sát | **Spark History Server** (số đo theo ứng dụng) + **`sar`** (CPU, bộ nhớ, đĩa, mạng theo máy, mỗi giây) | `bench/metrics.py` |
| Tái lập | Terraform (tạo máy) + Ansible + script cài/khởi động dùng chung (`cluster/install.sh`, `start-cluster.sh`); phiên bản cố định; dữ liệu tất định | `infra/`, `cluster/` |
| Cùng cấu hình | cấu hình Spark mặc định, giống nhau ở mọi lần chạy; chỉ codec thay đổi | `cluster/spark/spark-defaults.conf` |
| Thử trước ở local | 3 container Ubuntu cài bằng **cùng script** | `dev/` |

Vì sao Spark (không MapReduce): Spark có cấu hình nén shuffle riêng biệt (dễ tách hai mức), History Server cho sẵn số đo chi tiết, và Spark là nội dung chính của môn. Vì sao standalone (không YARN): đề không yêu cầu, ít thành phần hơn.

---

## §3. Ứng dụng và loại tải

**Đề nói gì.** Ba ứng dụng phân tán, mỗi cái một kiểu tải:
1. **I/O-bound**: bị giới hạn chủ yếu bởi đọc/ghi đĩa hoặc truyền dữ liệu, tính toán ít;
2. **CPU-bound**: tính toán nặng, thời gian chủ yếu là CPU chứ không phải di chuyển dữ liệu;
3. **Cân bằng**: vừa tính nhiều vừa di chuyển nhiều dữ liệu, không bên nào áp đảo.

Phải **đặc trưng hoá bằng thực nghiệm** ba app và **chứng minh bằng số đo CPU, đĩa, mạng** rằng chúng đúng kiểu. Với mỗi app: đánh giá nhiều thuật toán, nhiều kích thước dữ liệu, cả hai mức nén. Mục tiêu: hiểu **đặc tính tính toán của app ảnh hưởng thế nào** tới lợi/hại của nén.

**Nghĩa là phải làm gì.**
- Thiết kế ba app khác nhau rõ rệt về tỉ lệ CPU / dữ liệu di chuyển.
- Không chỉ tự nhận "app này I/O-bound", mà phải **đo** và đưa số liệu chứng minh.
- Mỗi app phải có **shuffle** (nếu không, nén truyền thông không có gì để nén).

**Project làm thế nào.**

| App | Việc làm | Vì sao đúng kiểu | File |
|---|---|---|---|
| `IoBound` | sắp xếp toàn bộ bảng `lineitem` (`sortByKey`) | đọc, shuffle, ghi toàn bộ dữ liệu; mỗi dòng gần như không tính | `apps/IoBound.java` |
| `CpuBound` | SHA-256 lặp 64 lần mỗi dòng, gộp theo 1000 nhóm (`reduceByKey`) | rất nhiều CPU; shuffle gần 0 nhờ gộp phía map | `apps/CpuBound.java` |
| `Balanced` | join `lineitem` với `orders` rồi tổng hợp (`join`, `reduceByKey`), SHA-256 8 lần | shuffle cả hai bảng và có tính toán | `apps/Balanced.java` |

Chứng minh: bảng `workload_characterisation.csv` với `cpu_ratio` (CPU / thời gian task) và **MB di chuyển trên mỗi giây CPU**, kèm %CPU, %iowait từ `sar`. Ở dev: io_bound ≈ 126, balanced ≈ 33, cpu_bound ≈ 13 MB/giây CPU, ba bậc tách biệt. Dữ liệu cho cả ba app: bảng kiểu TPC-H do project tự sinh (`datagen/`), để chọn được kích thước tuỳ ý và tái lập được.

---

## §4. Các thuật toán nén

**Đề nói gì.** Đánh giá nhiều thuật toán mà Hadoop/Spark hỗ trợ, gồm (nếu được hỗ trợ): **Snappy** (nhanh), **LZ4** (thông lượng cao), **Gzip** (nén mạnh, tốn CPU), **Zstd** (đánh đổi tuỳ chỉnh được). Một lần chạy **không nén** làm **mốc so sánh** (*baseline*). Và **trước hết** phải đánh giá **đặc tính riêng** của các thuật toán: tỉ lệ nén, thời gian nén, thời gian giải nén, chi phí CPU.

**Nghĩa là phải làm gì.**
- Đủ 4 codec + không nén, ở mỗi mức **nếu mức đó hỗ trợ** ("where supported").
- Một phép đo **riêng cho thuật toán**, tách khỏi cluster (không có HDFS, mạng, Spark), để biết bản chất từng codec.

**Project làm thế nào.**

| Codec | Mức lưu trữ (Hadoop) | Mức shuffle (Spark) |
|---|---|---|
| không nén | `none` | `off` |
| Gzip | `GzipCodec` | **không hỗ trợ** (Spark không có gzip cho shuffle) → ghi rõ trong báo cáo |
| Snappy | `SnappyCodec` | `snappy` |
| LZ4 | `Lz4Codec` | `lz4` |
| Zstd | `ZStandardCodec` (cần thư viện native, chỉ chạy trên EC2 x86) | `zstd` |

Đặc tính riêng: **micro-benchmark** `bench/CodecBench.java`, nén/giải nén một file dữ liệu thật và 64 MB dữ liệu ngẫu nhiên trên một máy, đo tỉ lệ, thời gian nén, thời gian giải nén, và **thời gian CPU** nén/giải nén (7 codec, gồm zstd ở 3 mức).

---

## §5. Đánh giá thực nghiệm

### §5.1 Nén mức lưu trữ

**Đề nói gì.** Đánh giá tác động của nén dữ liệu trên HDFS. Với mỗi thuật toán, đo: **kích thước dataset sau nén**; **chi phí nén và giải nén**; **thời gian chạy** của các app; **CPU** (mức dùng và tổng tiêu thụ); **khối lượng và thông lượng I/O đĩa**. Làm với **nhiều kích thước dữ liệu**. Phải giải thích **giảm I/O đĩa có bù được chi phí CPU** của nén không.

**Project làm thế nào:** thí nghiệm **E1**: dữ liệu ghi sẵn trên HDFS ở 5 bản (none, gzip, snappy, lz4, zstd); mỗi app đọc từng bản và ghi output cùng codec; **tắt nén shuffle** để chỉ đo nén lưu trữ.

| Đề đòi đo | Lấy từ |
|---|---|
| kích thước dataset sau nén | `hdfs dfs -du` → `results/<env>/datasets/sf<N>.txt` → tỉ lệ nén (`fig5`) |
| chi phí nén/giải nén | micro-benchmark (`fig5b`) + CPU tăng thêm so với không nén (`fig2`) |
| thời gian chạy | `duration_ms` (History Server) → `fig1` |
| CPU | `executor_cpu_ns` (History Server) → `fig2`; `cpu_busy_pct` (sar) |
| I/O đĩa (khối lượng) | `disk_read_bytes` + `disk_write_bytes` (sar) → `fig3` |
| I/O đĩa (thông lượng) | khối lượng / thời gian (tính được từ hai cột trên) |

### §5.2 Nén mức truyền thông

**Đề nói gì.** Đánh giá nén dữ liệu trung gian giữa các task, nhất là **shuffle**. Đo: **thời gian chạy**; **lượng dữ liệu qua mạng**; **thông lượng mạng**; **chi phí CPU** của nén/giải nén; **thời gian shuffle**. Xác định **giảm lưu lượng mạng có cải thiện hiệu năng không**, và **khi nào nén phản tác dụng**.

**Project làm thế nào:** thí nghiệm **E2**: luôn đọc bản **không nén**, đổi codec shuffle (`off`, lz4, snappy, zstd) bằng `--conf spark.shuffle.compress`, `spark.shuffle.spill.compress`, `spark.io.compression.codec`.

| Đề đòi đo | Lấy từ |
|---|---|
| thời gian chạy | `duration_ms` → `fig1` |
| dữ liệu qua mạng | `net_rx_bytes` (sar, mọi máy) → `fig4`; `shuffle_remote_read_bytes` (History Server) |
| thông lượng mạng | lưu lượng / thời gian; có dữ liệu từng giây trong `results/<env>/sar/` |
| chi phí CPU | `executor_cpu_ns` → `fig2` |
| thời gian shuffle | `shuffle_write_ns` (thời gian ghi shuffle, gồm nén) và `shuffle_fetch_wait_ms` (thời gian chờ nhận) |
| byte shuffle (tỉ lệ nén shuffle) | `shuffle_write_bytes` → `fig5` |

### §5.3 Nén kết hợp

**Đề nói gì.** Nghiên cứu tương tác giữa hai mức, so sánh ít nhất 4 cấu hình:

| | Nén lưu trữ | Nén truyền thông |
|---|---|---|
| C0 | tắt | tắt |
| C1 | bật | tắt |
| C2 | tắt | bật |
| C3 | bật | bật |

Xác định bật cả hai **có thêm lợi** hay chỉ **thêm chi phí CPU thừa**.

**Project làm thế nào:** thí nghiệm **E3**: chọn một codec lưu trữ X và một codec shuffle Y (`combined` trong YAML; mặc định zstd và lz4), chạy 4 tổ hợp (none/off, X/off, none/Y, X/Y). Đồ thị `fig6`.

### §5.4 Ảnh hưởng của kích thước dữ liệu

**Đề nói gì.** Mọi thí nghiệm chính phải lặp lại với **nhiều kích thước**, khác nhau đủ lớn để thấy hành vi thay đổi, ví dụ 1, 5, 10, 20 GB (tuỳ hạ tầng). Xem **thứ hạng các codec có thay đổi** khi dữ liệu lớn lên không.

**Project làm thế nào:** kích thước = **scale factor** (SF) của bộ sinh dữ liệu; SF 1 ≈ 1 GB text. Đợt chính thức: SF **1, 5, 10, 20** (`sizes` trong `experiments-aws.yaml`). Mọi đồ thị fig1–fig4 có trục x là kích thước.

---

## §6. Kết quả và phân tích

**Đề nói gì.**
- Sản phẩm chính: **đánh giá định lượng**, kèm **đường cong hiệu năng**. Tối thiểu các đồ thị:

| Đồ thị đề yêu cầu | So sánh | Trong project |
|---|---|---|
| Thời gian theo kích thước | các thuật toán | `fig1_time_s_*` |
| CPU theo kích thước | chi phí nén | `fig2_cpu_s_*` |
| I/O đĩa theo kích thước | nén lưu trữ | `fig3_disk_gb_*` |
| Lưu lượng mạng theo kích thước | nén truyền thông | `fig4_net_gb_*` |
| Tỉ lệ nén theo thuật toán | hiệu quả nén | `fig5_compression_ratio`, `fig5b_codec_microbench` |
| Thời gian theo cấu hình | C0, C1, C2, C3 | `fig6_configurations` |
| Thời gian theo kiểu tải | I/O, CPU, cân bằng | `fig7_workload` |

- Mỗi thí nghiệm **lặp lại nhiều lần**; đồ thị có **đơn vị, chú thích, chỉ báo dao động** (error bar).
- Không chỉ đưa đồ thị: phải **phân tích và giải thích**, trả lời 7 câu hỏi:
  1. Thuật toán nào cân bằng tốt nhất giữa tỉ lệ nén và chi phí CPU?
  2. Nén có **luôn** cải thiện app I/O-bound không?
  3. Nén ảnh hưởng app CPU-bound thế nào?
  4. Lợi ích của nén thay đổi thế nào khi dữ liệu lớn lên?
  5. Nén có lợi hơn cho lưu trữ hay cho truyền thông?
  6. Kết hợp hai mức có thể **làm chậm** không?
  7. Khi nào nên **tắt** nén?
- Kết luận phải dựa trên **bằng chứng thực nghiệm**.

**Project làm thế nào.** Lặp mỗi cấu hình 3 lần, thứ tự xáo trộn; error bar = độ lệch chuẩn (`analysis/report.py`). Bảng nối mỗi câu hỏi với đồ thị/bảng dùng để trả lời: xem [06-phan-tich.md](06-phan-tich.md), mục 6. Câu trả lời cuối cùng viết trong báo cáo, từ số liệu AWS.

---

## §7. Sản phẩm phải nộp

| Đề yêu cầu | Trong project |
|---|---|
| **Hạ tầng thí nghiệm**: môi trường chạy được, script triển khai, file cấu hình, công cụ giám sát | `infra/` (Terraform, Ansible), `cluster/` (script cài/khởi động, cấu hình), `dev/` (bản local), `bench/metrics.py` + `sar` + History Server |
| **Kết quả thí nghiệm**: số liệu thô, script thí nghiệm, mọi đồ thị; tái lập được | `results/aws/` (sau khi chạy), `bench/`, `analysis/report.py` → `analysis/figures/aws/` |
| **Báo cáo kỹ thuật**: phương pháp, workload, cấu hình nén, kết quả, phân tích; đặc biệt giải thích chi phí nén và khác biệt giữa các workload | `report/` (chưa viết) |
| **Thuyết trình**: kết quả chính, đồ thị; **biện minh được các lựa chọn** và **giải thích kết quả bất ngờ** | `report/` (chưa làm); [../QA.md](../QA.md) chuẩn bị câu hỏi |

---

## Bảng kiểm tra toàn bộ yêu cầu

| Mục | Yêu cầu | Trạng thái |
|---|---|---|
| §2 | HDFS phân tán, Spark, nhiều worker | ✅ |
| §2 | Giám sát CPU, đĩa, mạng | ✅ (`sar`, History Server) |
| §2 | Giám sát bộ nhớ | ✅ (`sar -r`: `mem_used_pct`, `mem_max_pct`) |
| §2 | Tái lập, cùng cấu hình khi so sánh | ✅ |
| §3 | 3 app I/O, CPU, cân bằng + chứng minh bằng số đo | ✅ (bảng đặc tính workload) |
| §4 | Snappy, LZ4, Gzip, Zstd + không nén | ✅ (gzip không có cho shuffle: Spark không hỗ trợ) |
| §4 | Đặc tính riêng: tỉ lệ, thời gian nén, thời gian giải nén | ✅ (micro-benchmark) |
| §4 | Đặc tính riêng: chi phí CPU | ✅ (`comp_cpu_ns`, `decomp_cpu_ns` → giây CPU/GB) |
| §5.1 | Kích thước sau nén, thời gian, CPU, I/O đĩa | ✅ |
| §5.2 | Thời gian, lưu lượng mạng, CPU, thời gian shuffle | ✅ |
| §5.3 | C0–C3 | ✅ |
| §5.4 | Nhiều kích thước (1, 5, 10, 20 GB) | ✅ thiết kế sẵn; ⬜ chưa chạy trên AWS |
| §6 | 7 đồ thị, lặp lại, error bar | ✅ (script); ⬜ chưa có số liệu AWS |
| §6 | Trả lời 7 câu hỏi bằng số liệu | ⬜ báo cáo |
| §7 | Báo cáo, slide | ⬜ |

---

## Từ yêu cầu đến thiết kế: đọc tiếp theo thứ tự

1. [01-tong-quan.md](01-tong-quan.md): khái niệm nền, kiến trúc, quy trình, một lần đo đi qua những gì.
2. [02-du-lieu.md](02-du-lieu.md): dữ liệu cho §3, §5.4.
3. [03-ung-dung.md](03-ung-dung.md): ba app của §3.
4. [04-ha-tang.md](04-ha-tang.md): hạ tầng của §2.
5. [05-do-luong.md](05-do-luong.md): đo lường cho §4, §5.
6. [06-phan-tich.md](06-phan-tich.md): đồ thị và câu hỏi của §6.
