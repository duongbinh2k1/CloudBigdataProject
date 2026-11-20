# Kế hoạch — Đánh giá thực nghiệm nén dữ liệu trong Spark

Đề bài: `CoursCloudN7/ProjectUSTH.pdf`. Lý do của từng lựa chọn: [DECISIONS.md](DECISIONS.md). Số liệu dev và giới hạn: [NOTES.md](NOTES.md). Chạy trên AWS: [AWS.md](AWS.md).

## 1. Hai mức nén

| Mức | Nén cái gì | Cách đặt |
|---|---|---|
| Lưu trữ (§5.1) | file text input/output trên HDFS | `rdd.saveAsTextFile(path, GzipCodec.class)`… Khi đọc, Hadoop nhận codec theo đuôi file |
| Truyền thông (§5.2) | dữ liệu shuffle giữa các node | `spark.shuffle.compress`, `spark.shuffle.spill.compress`, `spark.io.compression.codec` |

Codec: none, gzip, snappy, lz4, zstd. Gzip **không có** cho shuffle trong Spark.

## 2. Hạ tầng

| Môi trường | Dùng để | Công nghệ |
|---|---|---|
| Dev | viết và thử code | Podman Compose trên laptop: 1 master + 2 worker container |
| AWS (Singapore) | số liệu của báo cáo | 4 máy EC2 m5.xlarge (1 master + 3 worker) — Terraform + Ansible; test trước bằng m7i-flex.large (Free plan) |

```
master  : NameNode + Spark Master + History Server (+ driver, run_experiments.py)
workerN : DataNode + Spark Worker
```

## 3. Ba ứng dụng (Java, RDD)

| App | Việc làm | Kiểu |
|---|---|---|
| `IoBound` | `sortByKey` toàn bộ `lineitem` rồi ghi lại | I/O: đọc, shuffle và ghi toàn bộ dữ liệu, gần như không tính toán |
| `CpuBound` | SHA-256 lặp 64 lần mỗi dòng + `reduceByKey` trên 1000 nhóm | CPU: shuffle gần như bằng 0 |
| `Balanced` | `lineitem` `join` `orders`, doanh thu + SHA-256 lặp 8 lần, `reduceByKey` | vừa shuffle lớn vừa tính toán |

## 4. Thí nghiệm (`bench/experiments-aws.yaml`)

| | Codec lưu trữ | Codec shuffle |
|---|---|---|
| E1 lưu trữ | none, gzip, snappy, lz4, zstd | off |
| E2 truyền thông | none | off, lz4, snappy, zstd |
| E3 kết hợp | C0 none/off · C1 X/off · C2 none/Y · C3 X/Y — mặc định X = zstd, Y = lz4, có thể đổi sau E1/E2 | |

- 3 app × kích thước SF 1, 5, 10, 20 (≈ GB text) × 5 lần lặp → **540 lần chạy** (cấu hình chung giữa E1/E2/E3 chỉ chạy một lần).
- Thứ tự xáo trộn (seed cố định), 1 lần khởi động mỗi app, xoá page cache mọi node trước mỗi lần chạy.

## 5. Số đo mỗi lần chạy (`runs.csv`)

| Nguồn | Số đo |
|---|---|
| Spark History Server | thời gian, CPU executor, input/output, shuffle ghi/đọc/đọc qua mạng, chờ fetch, GC, spill |
| `sar` trên mọi node | %CPU, %iowait, byte đọc/ghi đĩa, byte mạng nhận/gửi |
| HDFS (`hdfs dfs -du`) | kích thước output; kích thước từng bản dataset (`results/<env>/datasets/`) |
| `CodecBench.java` | tỉ lệ nén, tốc độ nén/giải nén trên một node |

## 6. Đồ thị và bảng (§6) — `analysis/report.py`

Kết quả ở `analysis/figures/<env>/`, mỗi đồ thị có bản PNG và PDF, error bar = độ lệch chuẩn qua 5 lần lặp.

| Đề yêu cầu | File |
|---|---|
| 1. Thời gian theo kích thước | `fig1_time_s_storage`, `fig1_time_s_shuffle` |
| 2. CPU theo kích thước | `fig2_cpu_s_storage`, `fig2_cpu_s_shuffle` |
| 3. I/O đĩa theo kích thước | `fig3_disk_gb_storage` (+ `_shuffle`) |
| 4. Lưu lượng mạng theo kích thước | `fig4_net_gb_shuffle` (+ `_storage`) |
| 5. Tỉ lệ nén theo thuật toán | `fig5_compression_ratio` (lưu trữ + shuffle), `fig5b_codec_microbench` (tỉ lệ / tốc độ) |
| 6. Thời gian theo cấu hình C0–C3 | `fig6_configurations` |
| 7. Thời gian theo kiểu workload | `fig7_workload` (speedup % so với không nén, ở kích thước lớn nhất) |

Đồ thị chính làm nổi bật **LZ4 và Zstd** (một codec nhanh, một codec nén mạnh, có ở cả hai mức); thư mục `all/` có đủ 4 codec.

Bảng (`tables/`): `summary.csv` (trung bình, độ lệch chuẩn của mọi số đo), `speedup.csv`, `ratio_storage.csv`, `ratio_shuffle.csv`, `codec_microbench.csv`, `workload_characterisation.csv` (cpu_ratio, MB/giây CPU, %CPU, %iowait — chứng minh kiểu của từng app, §3).

## 7. Bảy câu hỏi (§6) và số liệu để trả lời

| Câu hỏi | Dựa vào |
|---|---|
| 1. Codec nào cân bằng tốt nhất giữa tỉ lệ nén và CPU? | `fig5b` + `fig5`, `fig2` (CPU tăng thêm khi nén) |
| 2. Nén có luôn giúp app I/O-bound? | `fig1` panel I/O-bound, `speedup.csv` (có codec nào âm không) |
| 3. Ảnh hưởng tới app CPU-bound? | `fig1`/`fig2` panel CPU-bound; shuffle của app này gần như 0 |
| 4. Lợi ích thay đổi thế nào khi dữ liệu lớn lên? | độ dốc các đường trong `fig1`, `speedup.csv` theo `sf` |
| 5. Nén có lợi hơn cho lưu trữ hay cho mạng? | so `fig7` hai panel, `fig3` với `fig4`, `shuffle_fetch_wait_ms` trong `summary.csv` |
| 6. Kết hợp hai mức có thể làm chậm đi không? | `fig6`: C3 so với C1 và C2 |
| 7. Khi nào nên tắt nén? | các speedup âm, `fig5b` (dữ liệu ngẫu nhiên), app CPU-bound |

## 8. Tiến độ

| Bước | Trạng thái |
|---|---|
| Cluster dev, bộ sinh dữ liệu, 3 app, micro-benchmark, script chạy, phân tích | ✅ chạy đầy đủ trên dev (24/24 lần chạy) |
| Hạ tầng AWS (Terraform + Ansible) | ✅ viết xong, `terraform validate` và kiểm tra cú pháp playbook OK — chưa chạy thật |
| Chạy thí nghiệm trên AWS | ⬜ (xem [AWS.md](AWS.md)) |
| Báo cáo + slide (`report/`) | ⬜ |

## 9. Rủi ro

| Rủi ro | Xử lý |
|---|---|
| Page cache che mất I/O đĩa | xoá cache trước mỗi lần chạy |
| Kết quả dao động | 5 lần lặp, error bar, thứ tự xáo trộn |
| Hết tiền AWS (~1 USD/giờ khi bật) | Budget Alert, chỉ bật cluster khi chạy, `make down ENV=aws` khi xong |
| Quota vCPU thấp (cần 16) | xin tăng quota sớm |
| Đầy đĩa | 200 GB/node, output xoá ngay sau mỗi lần đo |
| zstd lưu trữ không chạy trên EC2 | lộ ra ngay ở bước sinh dữ liệu đầu tiên của `make run ENV=aws` (xem AWS.md); nếu không sửa được thì bỏ zstd ở mức lưu trữ và nêu trong báo cáo |
