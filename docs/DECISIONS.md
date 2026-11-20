# Các lựa chọn kỹ thuật và lý do

**Spark thay vì MapReduce.** Spark có cấu hình nén shuffle riêng, tách khỏi nén lưu trữ, nên đo được từng mức. History Server cho sẵn số byte shuffle và thời gian CPU.

**Spark standalone, Hadoop chỉ dùng HDFS.** Ít daemon hơn YARN và đề bài không yêu cầu YARN.

**Java + API RDD, file text.** Đúng như slide và TP của môn. Nén lưu trữ là nén cả file bằng codec Hadoop (`saveAsTextFile(path, codec)`). Không dùng Parquet: Parquet cần Spark SQL/DataFrame (không có trong môn) và nén theo từng trang nhỏ, khó giải thích hơn. Python chỉ dùng để điều phối thí nghiệm và vẽ đồ thị, không chạy trong Spark.

**Dữ liệu kiểu TPC-H tự sinh.** Hai bảng `orders` và `lineitem`, CSV phân tách bằng `|`. Mỗi giá trị là hàm băm của (seed, cột, khoá), nên cùng tham số thì cho cùng dữ liệu, với bất kỳ số node nào. Dữ liệu "giống thật" nên nén được (dữ liệu ngẫu nhiên thì không), và có khoá để join. SF 1 ≈ 1 GB.

**150 000 đơn hàng mỗi file.** File text nén **không chia nhỏ được**: 1 file = 1 task. Mỗi file `lineitem` ≈ 85 MB, nhỏ hơn một block HDFS (128 MB), nên mọi codec, kể cả `none`, đều có cùng số task và chỉ có nén là thay đổi. Trong báo cáo cần nêu: với file lớn, nén không chia nhỏ được sẽ làm mất song song khi đọc.

**zstd lưu trữ = `ZStandardCodec` của Hadoop.** Codec này cần thư viện native `libhadoop.so` + `libzstd`. Bản Hadoop chính thức chỉ có thư viện này cho x86_64, nên dùng được trên EC2 nhưng không chạy trên Mac ARM: ở dev chỉ dùng none/gzip/snappy/lz4. Ansible cài `libzstd1`, đặt `extraLibraryPath` và kiểm tra `hadoop checknative`.

**Tài nguyên Spark tính từ máy, cố định trong mọi lần chạy.** Ansible đọc số core và RAM của máy (facts, mọi máy cùng loại) rồi điền vào template: một executor mỗi worker, dùng hết core và **một nửa RAM**; driver **một phần tư RAM**; mỗi worker cho Spark RAM − 3 GB (phần còn lại cho Linux và DataNode); `spark.default.parallelism` = số worker × số core × 4; số bản sao HDFS = min(3, số worker). Ví dụ m5.xlarge: 4 core, executor 7 GB, driver 3 GB, 48 partition; m7i-flex.large: 2 core, 3 GB, 1 GB, 24. Ở dev các giá trị nằm trong `dev/conf/spark/`. Với RDD, `parallelism` là số partition của mọi shuffle, nên giữ cố định để giữa các lần chạy chỉ có nén thay đổi. Mỗi dòng của `runs.csv` ghi lại các giá trị Spark thực sự dùng.

**Ba app tách biệt rõ.** Đo bằng lượng dữ liệu di chuyển trên mỗi giây CPU: io_bound ≈ 126 MB/s CPU, balanced ≈ 33, cpu_bound ≈ 13 (ở dev). Số vòng SHA-256 (64 và 8) được chỉnh để đạt được khoảng cách này.

**Giám sát: Spark History Server + `sar`.** History Server (REST API) cho số đo theo ứng dụng: thời gian CPU của executor, byte shuffle ghi/đọc/qua mạng, thời gian chờ fetch. `sar` (sysstat) cho số đo theo máy: %CPU, %iowait, byte đọc/ghi đĩa, byte mạng. Cả hai có sẵn, không cần cài thêm hệ thống giám sát như Prometheus/Grafana.

**Micro-benchmark bằng chính lớp codec của Spark** (`CodecBench.java`): đo đúng code mà Spark dùng để nén shuffle, trên một máy, không có HDFS hay mạng, để thấy đặc tính riêng của từng codec (§4 của đề).

**E3 dùng zstd (lưu trữ) + lz4 (shuffle)** làm mặc định: một codec nén mạnh cho dữ liệu nằm lâu trên đĩa, một codec nhanh cho dữ liệu chỉ đi qua mạng một lần. Có thể đổi sau khi xem E1/E2.

**Ba lớp cấu hình, mỗi thông số ở một chỗ.** Terraform (`variables.tf`): máy nào, bao nhiêu máy. Ansible: cài đặt, và tài nguyên tính tự động từ máy. YAML (`bench/experiments-<env>.yaml`): chỉ thiết kế thí nghiệm. Đổi phần cứng thì không phải sửa YAML, đổi thí nghiệm thì không phải sửa hạ tầng.

**Phương pháp đo.** 5 lần lặp, thứ tự xáo trộn bằng seed (để khoảng thời gian cluster chậm không luôn rơi vào cùng một codec), 1 lần khởi động mỗi app, xoá page cache trên mọi node, chạy tiếp được nếu bị ngắt.

**Hạ tầng AWS.** 1 master + 3 worker `m5.xlarge` (4 vCPU, 16 GB), đĩa gp3 200 GB, Ubuntu 22.04, vùng Singapore (`ap-southeast-1`, gần Việt Nam nhất), VPC mặc định, cả 4 máy trong **cùng một availability zone** (mạng giống nhau giữa các máy, không mất phí truyền dữ liệu giữa các zone). Không dùng t3 (credit CPU cạn dần), máy "flex" (không đảm bảo 100% CPU) hay gp2 (credit I/O), vì hiệu năng sẽ thay đổi trong lúc đo. Riêng giai đoạn test dùng `m7i-flex.large` vì nó có trong Free plan; số đo của giai đoạn này không vào báo cáo. Ít nhất 2 máy theo yêu cầu của thầy, để shuffle đi qua mạng thật. Dung lượng: SF 1+5+10+20 với 5 codec ≈ 88 GB trên mỗi worker (3 bản sao), cộng thêm file shuffle.

**Cài trực tiếp bằng Ansible, không dùng container trên AWS.** Không có mạng overlay xen vào số đo mạng. Thứ tự giống TP: cấu hình, rồi `start-dfs.sh`, rồi `start-master.sh` / `start-workers.sh`. Ở laptop thì dùng container (Podman Compose) vì chỉ để phát triển.

**Đo 4 thuật toán, vẽ nổi bật 2 (LZ4 + Zstd).** Đây là một codec nhanh và một codec nén mạnh, có mặt ở cả hai mức. Gzip chỉ có ở mức lưu trữ.

**Không giả lập mạng chậm.** Đề bài không yêu cầu. Mạng m5 nhanh (nền ~1,25 Gbps, burst tới 10 Gbps) nên lợi ích của nén shuffle có thể nhỏ; cần giải thích bằng thời gian chờ fetch và lưu lượng mạng.

| Phần mềm | Phiên bản |
|---|---|
| Java | 11 |
| Hadoop | 3.4.1 |
| Spark | 3.5.5 (bản kèm Hadoop 3) |
