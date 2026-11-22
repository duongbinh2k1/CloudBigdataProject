# Ghi chú — số liệu trên cluster dev và các điểm cần nêu trong báo cáo

> Cluster dev = 3 container trên cùng một laptop (dùng chung CPU, đĩa, mạng ảo): số liệu ở đây
> chỉ để kiểm tra code và để có thứ tự độ lớn. Số liệu của báo cáo lấy từ AWS.

## Kích thước dataset (SF 1, `results/dev/datasets/sf1.txt`)

| Codec | orders + lineitem | Tỉ lệ so với none |
|---|---|---|
| none | 933 MB | 1.00× |
| gzip | 283 MB | 3.30× |
| snappy | 454 MB | 2.05× |
| lz4 | 478 MB | 1.95× |
| zstd | (chỉ đo được trên EC2) | |

`orders` 1 500 000 dòng, `lineitem` 5 998 152 dòng, 10 file mỗi bảng.

## Micro-benchmark codec (trung vị 5 lần, 20/11)

Một file `lineitem` (75 MB text) và 64 MB byte ngẫu nhiên, trên một container. MB = 10⁶ byte.

| Codec | Tỉ lệ | Nén MB/s | Giải nén MB/s | Nén dữ liệu ngẫu nhiên MB/s |
|---|---|---|---|---|
| lz4 | 1.87 | 360 | 1237 | 2002 |
| snappy | 1.96 | 502 | 866 | 5573 |
| lzf | 2.04 | 244 | 933 | 141 |
| zstd-1 | 3.02 | 324 | 762 | 2977 |
| zstd-3 | 3.14 | 186 | 629 | 2410 |
| zstd-9 | 3.50 | 26 | 739 | 1113 |
| gzip-6 | 3.29 | 27 | 321 | 54 |

- zstd-1 (mức Spark dùng cho shuffle) nén gần bằng gzip (3.02 so với 3.29) nhưng nhanh hơn ~12 lần.
- gzip-6 và zstd-9 chậm hơn lz4 ~13 lần; zstd-9 chỉ nén hơn zstd-3 khoảng 10%.
- lz4/snappy nhanh nhất nhưng chỉ nén ~1.9×.
- Dữ liệu ngẫu nhiên (không nén được, câu hỏi 7): lz4/snappy/zstd đi qua ở tốc độ GB/s nên tốn rất ít; gzip (54 MB/s) và lzf (141 MB/s) vẫn tốn CPU mà không được gì.
- Thứ tự tỉ lệ nén khớp với kích thước dataset thật (gzip 3.30×, snappy 2.05×, lz4 1.95×).

## Đặc tính ba ứng dụng (không nén, SF 1, đo ngày 15/11)

| App | cpu_ratio | MB di chuyển / giây CPU | Shuffle ghi |
|---|---|---|---|
| io_bound | 0.61 | 126 | 937 MB |
| balanced | 0.67 | 33 | 641 MB |
| cpu_bound | 0.84 | 13 | ~1 MB |

`cpu_ratio` = thời gian CPU / thời gian chạy của task. MB di chuyển = input + shuffle đọc + shuffle ghi + output.
Với 32 vòng SHA-256, `cpu_bound` (20) và `balanced` (31) quá gần nhau, nên tăng `cpu_bound` lên 64 vòng.
Ở SF 0.1 (`make run`, bảng `workload_characterisation.csv`), thứ tự vẫn giữ nguyên: 74 / 21 / 9 MB/giây CPU.

## Chạy test trên AWS (22/11, `results/aws-test/`, không dùng cho báo cáo)

4 × `m7i-flex.large` (2 vCPU, 8 GB) ở Sydney, SF 1, 1 lần mỗi cấu hình, 27/27 lần chạy `ok`. Máy "flex" và 1 lần lặp: chỉ để kiểm tra cả quy trình.

- **Cả quy trình chạy được trên EC2**: Terraform + Ansible (~12 phút), sinh dữ liệu, micro-benchmark, 27 lần đo, `make fetch`, `make down`.
- **zstd lưu trữ chạy được**: Spark (client Hadoop 3.3.4) ghi và đọc file `.zst` bằng thư viện native của Hadoop 3.4.1.
- Cấu hình Spark tự tính đúng: executor 2 core / 3 GB, driver 1 GB, `parallelism` 24 (cột trong `runs.csv`).
- Xoá cache có tác dụng: `disk_read_bytes` > 0 ở mọi lần chạy.
- Tỉ lệ nén lưu trữ (SF 1): gzip 3,30×, zstd 3,17×, snappy 2,05×, lz4 1,95×. Shuffle: zstd ~3,3×, lz4 ~2,0–2,4×, snappy ~2,1–2,4×.
- Quan sát cần xác nhận bằng đợt chính thức (5 lần lặp, `m5.xlarge`):
  - nén shuffle làm `balanced` nhanh hơn ~17% (43 s → 36 s) và **giảm** tổng CPU (156 s → 114 s); không có spill, GC như nhau. Giả thuyết: ghi/đọc 2,4 lần ít byte shuffle hơn tốn ít CPU hệ điều hành hơn chi phí nén LZ4;
  - nén lưu trữ gần như không giúp `io_bound` ở SF 1; gzip làm chậm 23%, zstd chậm 7%;
  - `cpu_bound` thay đổi < 6% với mọi cấu hình.

## Điểm cần nêu trong báo cáo

- `sortByKey` chạy thêm một job lấy mẫu đọc toàn bộ input để chia khoảng, nên `io_bound` đọc input **hai lần**.
- File text: mọi app đọc **cả dòng**, kể cả khi chỉ dùng vài cột.
- `reduceByKey` gộp sẵn ở phía map (như combiner của Hadoop), nên shuffle của `cpu_bound` gần như bằng 0: nén shuffle gần như không tác động tới app này.
- File nén không chia nhỏ được (xem DECISIONS.md).
- Mạng EC2 nhanh, nên lợi ích của nén shuffle có thể nhỏ (xem DECISIONS.md).
- CLI `hdfs dfs -text` không đọc được file `.lz4` (thiếu lz4-java trong classpath của Hadoop); Spark thì đọc được bình thường.
- Trong container dev không xoá được page cache (cảnh báo khi chạy). Trên EC2 thì xoá được (`sudo`).
- `results/dev/` không được commit (`.gitignore`): chỉ là chạy thử.
