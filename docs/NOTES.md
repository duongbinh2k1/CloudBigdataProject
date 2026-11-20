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

## Micro-benchmark codec (`make codec-bench`, trung vị 5 lần, 20/11)

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
Ở SF 0.1 (`make experiments`, bảng `workload_characterisation.csv`), thứ tự vẫn giữ nguyên: 74 / 21 / 9 MB/giây CPU.

## Điểm cần nêu trong báo cáo

- `sortByKey` chạy thêm một job lấy mẫu đọc toàn bộ input để chia khoảng, nên `io_bound` đọc input **hai lần**.
- File text: mọi app đọc **cả dòng**, kể cả khi chỉ dùng vài cột.
- `reduceByKey` gộp sẵn ở phía map (như combiner của Hadoop), nên shuffle của `cpu_bound` gần như bằng 0: nén shuffle gần như không tác động tới app này.
- File nén không chia nhỏ được (xem DECISIONS.md).
- Mạng EC2 nhanh, nên lợi ích của nén shuffle có thể nhỏ (xem DECISIONS.md).
- CLI `hdfs dfs -text` không đọc được file `.lz4` (thiếu lz4-java trong classpath của Hadoop); Spark thì đọc được bình thường.
- Trong container dev không xoá được page cache (cảnh báo khi chạy). Trên EC2 thì xoá được (`sudo`).
- `results/dev/` không được commit (`.gitignore`): chỉ là chạy thử.
