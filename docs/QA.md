# Câu hỏi bảo vệ và câu trả lời

Các câu hỏi thầy có thể hỏi, nhóm theo chủ đề. Mỗi câu trả lời có phần **ngắn** (nói trước) và phần **chi tiết** (nếu thầy hỏi tiếp). Giải thích đầy đủ ở [GUIDE.md](GUIDE.md).

Mẹo chung: trả lời bằng **số liệu và code của project** ("trong `IoBound.java`, dòng `sortByKey`…", "bảng `speedup.csv` cho thấy…") thay vì lý thuyết chung.

---

## A. Tổng quan

**A1. Project của bạn làm gì?**
Ngắn: đo lợi ích và chi phí của nén dữ liệu trong Spark ở hai mức (file trên HDFS và dữ liệu shuffle), với 3 kiểu ứng dụng, 4 codec, 4 kích thước dữ liệu, trên cluster 4 máy EC2.
Chi tiết: nén giảm byte phải đọc/ghi/truyền nhưng tốn CPU. Project tìm xem khi nào lợi lớn hơn hại, bằng cách thay đổi từng yếu tố một (E1: nén lưu trữ, E2: nén shuffle, E3: kết hợp), lặp 5 lần mỗi cấu hình, và đo thời gian, CPU, đĩa, mạng.

**A2. Vì sao chọn Spark mà không phải Hadoop MapReduce?**
Spark có cấu hình nén shuffle riêng (`spark.shuffle.compress`, `spark.io.compression.codec`), tách khỏi nén file, nên đo được từng mức riêng. Spark History Server cung cấp sẵn byte shuffle, thời gian CPU của từng stage. Spark cũng là nội dung chính của môn (slide `3-spark`, TP bằng Java).

**A3. Vì sao Spark standalone mà không YARN?**
Đề không yêu cầu YARN. Standalone ít tiến trình hơn (không có ResourceManager, NodeManager), ít RAM, ít biến số gây nhiễu. Hadoop chỉ dùng cho HDFS.

**A4. Vì sao Java và RDD?**
Đúng như slide và TP của môn (WordCount bằng `JavaRDD`, `mapToPair`, `reduceByKey`). Python chỉ dùng cho phần điều phối thí nghiệm và vẽ đồ thị, không chạy trong Spark, nên không ảnh hưởng số đo.

**A5. Kết quả chính là gì?**
*(Điền sau khi có số liệu AWS.)* Trả lời theo thứ tự: codec tốt nhất cho từng mức, app nào được lợi, lợi bao nhiêu % ở SF 20, khi nào nén làm chậm. Dẫn đồ thị `fig7_workload` và `fig6_configurations`.

---

## B. Hạ tầng

**B1. Cluster gồm những gì?**
4 máy EC2 cùng loại ở Sydney, trong một VPC riêng, cùng một availability zone: 1 master (NameNode, Spark Master, History Server, chạy driver và script thí nghiệm) và 3 worker (mỗi máy một DataNode và một Spark Worker). DataNode và Worker đặt chung máy để Spark đọc block HDFS ngay tại chỗ (data locality).

**B2. Vì sao ít nhất 2 máy, vì sao không chạy tất cả trên 1 máy?**
Để shuffle đi qua mạng thật. Trên một máy, "mạng" chỉ là bộ nhớ nên nén truyền thông không có ý nghĩa. Ngoài ra các tiến trình dùng chung CPU và đĩa, số đo bị nhiễu.

**B3. Vì sao `m5.xlarge` mà không phải `t3`?**
`t3` là máy *burstable*: dùng CPU theo "credit", hết credit thì bị giới hạn CPU. Hiệu năng thay đổi giữa đầu và cuối đợt đo, trùng với thứ tự chạy → sai số. `m5` có CPU ổn định. Tương tự, đĩa `gp3` có hiệu năng cố định (3000 IOPS, 125 MB/s), còn `gp2` có credit I/O.

**B4. Còn máy `m7i-flex.large` thì sao?**
Chỉ để **test** cluster vì nó có trong Free plan của AWS. Máy "flex" không đảm bảo luôn có 100% CPU nên số đo trên đó không dùng cho báo cáo.

**B5. Terraform làm gì? Vì sao dùng?**
Tạo hạ tầng AWS bằng code (*Infrastructure as Code*, có trong slide và TP Terraform): mạng (VPC, subnet, internet gateway, bảng định tuyến), security group, key pair, 4 máy EC2, và file `inventory.ini` cho Ansible. Lợi ích: tái lập được (`terraform apply` lại ra đúng cluster), xoá sạch được (`terraform destroy`), không thao tác tay trên console. File: `infra/terraform/main.tf`.

**B5b. Vì sao chọn vùng Sydney?**
Tài khoản dùng Free plan của AWS, và tổ chức quản lý tài khoản chỉ cho phép vùng Sydney (`ap-southeast-2`); Singapore gần Việt Nam hơn nhưng bị chặn. Khoảng cách từ Việt Nam không ảnh hưởng số đo, vì mọi số đo là bên trong cluster (các máy cùng một availability zone); chỉ SSH từ laptop trễ hơn một chút.

**B6. Ansible làm gì?**
Cài đặt và cấu hình các máy đã tạo, qua SSH, theo trình tự của TP: cài Java và gói, tải Hadoop/Spark, điền file cấu hình từ template, SSH không mật khẩu, format NameNode, `start-dfs.sh`, `start-master.sh`, `start-workers.sh`, copy code, build jar. File: `infra/ansible/cluster.yml`.

**B7. Vì sao không dùng Docker / Kubernetes trên AWS?**
Container thêm một lớp mạng ảo (overlay) xen vào số đo mạng, mà mạng là thứ cần đo cho nén shuffle. HDFS trong container cũng hay gặp lỗi tên máy. Cài trực tiếp lên máy ảo giống cách làm trong TP. Ở laptop thì có dùng container (Podman) vì chỉ để phát triển.

**B8. Cấu hình Spark (core, RAM) được đặt thế nào? Đổi máy thì sao?**
Ansible đọc số core và RAM của máy (*facts*) rồi điền vào template: executor dùng hết core và ½ RAM, driver ¼ RAM, worker cho Spark RAM − 3 GB, `parallelism` = worker × core × 4, bản sao HDFS = min(3, worker). Đổi loại máy hay số worker chỉ cần sửa `infra/terraform/variables.tf`, các giá trị khác tự tính lại. File: `infra/ansible/vars.yml`, `templates/spark-defaults.conf`.

**B9. Facts là gì? Template là gì?**
*Facts* là thông tin Ansible tự thu thập khi kết nối vào máy (số CPU, RAM, hệ điều hành…), dùng như biến (`ansible_processor_vcpus`, `ansible_memtotal_mb`). *Template* là file cấu hình có chỗ trống `{{ biến }}` (Jinja2); module `template` điền giá trị rồi ghi file lên máy, giống ví dụ `template: src=httpd.j2` trong slide Ansible.

**B10. Bảo mật thế nào?**
Security group chỉ mở SSH (cổng 22) cho IP hiện tại của laptop; các cổng khác chỉ mở giữa các máy trong cluster. Không có khoá hay mật khẩu nào trong repo (`.gitignore` chặn `*.tfvars`, `*.tfstate`, `*.pem`, `inventory.ini`).

**B11. Giám sát cluster bằng gì?**
Spark History Server (số đo theo ứng dụng: thời gian, CPU, byte shuffle, chờ fetch) và `sar` (sysstat) trên mọi node (CPU, đĩa, mạng, mỗi giây một mẫu). Thêm các giao diện web: HDFS (9870), Spark Master (8080), History Server (18080).

**B12. Làm sao tái lập thí nghiệm?**
`make up ENV=aws` rồi `make run ENV=aws`. Phiên bản phần mềm cố định (Hadoop 3.4.1, Spark 3.5.5, Java 11), hạ tầng bằng code, dữ liệu tất định (seed 42), thứ tự chạy cố định (seed trong YAML), cấu hình thí nghiệm lưu cùng kết quả (`results/aws/config.yaml`).

**B13. Chi phí bao nhiêu?**
Khoảng 1 USD/giờ khi cluster bật (4 × m5.xlarge + đĩa), cả đợt đo khoảng 1 ngày ≈ 25–35 USD, nằm trong credit của tài khoản mới. Cluster được xoá ngay khi xong (`make down ENV=aws`).

---

## C. HDFS và Spark

**C1. Shuffle là gì? Vì sao nó quan trọng ở đây?**
Khi một phép biến đổi cần gom dữ liệu theo khoá (`sortByKey`, `reduceByKey`, `join`), Spark phải phân phối lại dữ liệu giữa các máy. Mỗi task ghi dữ liệu đã chia theo khoá xuống đĩa local (*shuffle write*), task ở stage sau tải phần của mình qua mạng (*shuffle read/fetch*). Đây chính là "dữ liệu trung gian" mà nén truyền thông (§5.2) nén lại.

**C2. Stage là gì? Mỗi app có mấy stage?**
Spark cắt chương trình tại mỗi shuffle. `IoBound`: job lấy mẫu + đọc/gán khoá (map) + sắp xếp/ghi (reduce). `CpuBound`: map (SHA-256) + reduce (gộp, ghi). `Balanced`: đọc lineitem, đọc orders, join + tính, reduceByKey + ghi.

**C3. `reduceByKey` khác `groupByKey` thế nào?**
`reduceByKey` gộp trước ở phía map (như *combiner* của MapReduce) rồi mới shuffle phần đã gộp → shuffle nhỏ. `groupByKey` shuffle mọi giá trị. Đó là lý do `CpuBound` gần như không có shuffle.

**C4. `spark.default.parallelism` là gì, vì sao giữ cố định?**
Số partition mặc định của mọi shuffle trong RDD. Nếu thay đổi giữa các lần chạy, thời gian thay đổi vì mức song song chứ không phải vì nén. Giá trị = worker × core × 4 (48 với 3 × m5.xlarge): mỗi core khoảng 4 task mỗi stage, đủ để cân bằng tải.

**C5. Data locality là gì?**
Spark ưu tiên chạy task trên máy đang chứa block dữ liệu, để không phải kéo block qua mạng. Vì vậy mỗi worker vừa là DataNode vừa là Spark Worker.

**C6. Replication 3 ảnh hưởng gì?**
Mỗi block được ghi 3 lần trên 3 máy, nên khi ghi output, dữ liệu đi qua mạng 2 lần thêm. Nén lưu trữ giảm luôn lượng dữ liệu nhân bản này.

**C7. Driver chạy ở đâu?**
Trên master (`spark-submit` mặc định ở *client mode*). Các app không gom dữ liệu về driver (chỉ ghi file), nên driver cần ít RAM.

---

## D. Nén

**D1. Hai mức nén khác nhau thế nào? Đặt ở đâu trong code?**
Lưu trữ: codec của Hadoop nén file trên HDFS. Ghi: `rdd.saveAsTextFile(path, GzipCodec.class)` (`Codecs.java`); đọc: Hadoop tự nhận theo đuôi file. Truyền thông: Spark nén file shuffle và dữ liệu truyền đi, bật bằng `--conf spark.shuffle.compress=true --conf spark.io.compression.codec=lz4` (`run_experiments.py`, hàm `submit_command`).

**D2. Vì sao không có gzip cho shuffle?**
Spark chỉ hỗ trợ `lz4`, `lzf`, `snappy`, `zstd` cho `spark.io.compression.codec`. Gzip quá chậm cho dữ liệu trung gian. Trong báo cáo ghi rõ: gzip chỉ có ở E1.

**D3. Vì sao đồ thị chính chỉ vẽ LZ4 và Zstd?**
Đo đủ 4 codec (đồ thị trong `all/`), nhưng đồ thị chính chỉ vẽ một codec nhanh (LZ4) và một codec nén mạnh (Zstd), cả hai có ở **cả hai mức**, để so sánh lưu trữ và truyền thông nhất quán và đồ thị dễ đọc.

**D4. Splittable là gì? Ảnh hưởng thế nào?**
File text nén bằng gzip/snappy/lz4/zstd phải đọc từ đầu, không cắt giữa được, nên 1 file = 1 task. Project giữ mỗi file < 1 block (150 000 đơn ≈ 85 MB < 128 MB) để mọi codec, kể cả `none`, có cùng số task. Trong thực tế, nén một file lớn bằng gzip làm mất song song khi đọc: đó là một chi phí của nén ngoài CPU.

**D5. Zstd lưu trữ cần gì đặc biệt?**
`ZStandardCodec` của Hadoop cần thư viện native `libhadoop.so` (bản Hadoop chính thức chỉ có cho Linux x86_64) và `libzstd`. Ansible cài `libzstd1`, đặt `spark.*.extraLibraryPath=/opt/hadoop/lib/native` và kiểm tra `hadoop checknative` phải báo `zstd: true`. Trên Mac (ARM) không có, nên dev chỉ dùng 4 codec còn lại.

**D6. Mức nén của Zstd là bao nhiêu?**
Shuffle: mức Spark mặc định (1). Lưu trữ: mức mặc định của `ZStandardCodec` (3). Micro-benchmark đo cả zstd-1, zstd-3, zstd-9 để thấy đánh đổi.

**D7. Micro-benchmark để làm gì? Khác thí nghiệm trên cluster thế nào?**
Đo **đặc tính riêng** của codec (tỉ lệ, tốc độ nén/giải nén) trên một máy, không có HDFS, mạng, Spark scheduling. Thí nghiệm cluster đo **tác động thật** lên ứng dụng. Micro-benchmark giải thích vì sao một codec làm nhanh hay chậm (ví dụ gzip nén 27 MB/s mỗi core).

**D8. Vì sao micro-benchmark có dữ liệu ngẫu nhiên?**
Để xem codec tốn bao nhiêu khi dữ liệu **không nén được** (đã nén sẵn, ảnh, video). LZ4/Snappy/Zstd phát hiện rất nhanh (GB/s); gzip vẫn chỉ ~54 MB/s, tức tốn CPU mà không được gì → trả lời câu "khi nào nên tắt nén".

---

## E. Dữ liệu

**E1. Dữ liệu ở đâu ra? Vì sao không dùng dataset có sẵn?**
Tự sinh, kiểu TPC-H (`orders`, `lineitem`). Lợi ích: kích thước tuỳ ý (SF 1 → 20), tái lập tuyệt đối, có khoá để join, dữ liệu "giống doanh nghiệp" nên nén được như thật. Dataset có sẵn thường cố định kích thước, phải tải về, khó tạo app join.

**E2. Làm sao đảm bảo sinh lại ra đúng dữ liệu cũ?**
Mỗi giá trị là hàm băm SplitMix64 của (seed 42, tên cột, khoá dòng), không dùng bộ sinh ngẫu nhiên tuần tự. Dòng thứ k chỉ phụ thuộc k, không phụ thuộc số partition hay số máy. Đã kiểm tra: sinh lại cho đúng từng byte.

**E3. SF là gì? SF 1 lớn bao nhiêu?**
*Scale factor* của TPC-H. SF 1 = 1,5 triệu đơn hàng, khoảng 6 triệu dòng lineitem, ≈ 933 MB text. SF 20 ≈ 19 GB.

**E4. Vì sao dạng text mà không Parquet?**
Text + `textFile` / `saveAsTextFile` đúng mô hình RDD của môn; nén lưu trữ là nén cả file, dễ giải thích. Parquet cần Spark SQL/DataFrame (không có trong môn) và nén theo từng trang nhỏ bên trong file.

**E5. Dữ liệu chiếm bao nhiêu chỗ?**
5 bản (none + 4 codec) × SF 1+5+10+20 ≈ 88 GB trên mỗi worker (3 bản sao chia cho 3 worker), cộng file shuffle → đĩa 200 GB mỗi máy.

---

## F. Ba ứng dụng

**F1. Ba app làm gì? Vì sao chúng đại diện cho 3 kiểu workload?**
- `IoBound`: sắp xếp toàn bộ lineitem theo ngày giao (`sortByKey`). Đọc, shuffle, ghi toàn bộ dữ liệu, gần như không tính toán.
- `CpuBound`: SHA-256 lặp 64 lần mỗi dòng, gộp theo 1000 nhóm (`reduceByKey`). Rất nặng CPU, shuffle ≈ 0.
- `Balanced`: join lineitem với orders, tính doanh thu, trễ giao, SHA-256 8 vòng, gộp theo (khách, ưu tiên). Shuffle cả hai bảng và có tính toán.

**F2. Làm sao chứng minh chúng đúng kiểu?**
Bằng số đo, bảng `workload_characterisation.csv` (lần chạy không nén): `cpu_ratio` (CPU / thời gian chạy task) và MB di chuyển trên mỗi giây CPU. Ở dev SF 1: io_bound 126, balanced 33, cpu_bound 13 MB/giây CPU, cách nhau 3–4 lần mỗi bậc; `cpu_ratio` cao nhất ở cpu_bound (0,84). Trên AWS bổ sung %CPU và %iowait từ `sar`.

**F3. Vì sao SHA-256 lặp 64 và 8 lần?**
Để điều chỉnh độ nặng CPU. Với 32 vòng, cpu_bound (20 MB/giây CPU) và balanced (31) quá gần nhau; tăng lên 64 vòng thì cpu_bound còn 13, tách rõ ba bậc. Có thể đổi bằng `rounds` trong YAML.

**F4. Vì sao doanh thu tính bằng số nguyên?**
Giá có 2 chữ số thập phân, chiết khấu và thuế là phần trăm, nên cent × % × % là số nguyên chính xác. Cộng hàng triệu số thực (`double`) thì kết quả phụ thuộc thứ tự cộng (khác giữa các lần chạy), cộng số nguyên thì luôn chính xác.

**F5. Vì sao `IoBound` đọc input 2 lần?**
`sortByKey` dùng *range partitioning*: Spark chạy trước một job lấy mẫu khoá trên toàn bộ input để chia các khoảng sao cho đều nhau, rồi mới chạy job sắp xếp thật. Đây là đặc điểm của Spark, nêu trong báo cáo.

**F6. Output các app ghi bằng codec nào?**
Cùng codec lưu trữ với input: nén lưu trữ áp dụng cho cả đọc lẫn ghi. Output được xoá ngay sau khi đo để không đầy đĩa.

---

## G. Phương pháp đo

**G1. Mỗi cấu hình chạy mấy lần? Vì sao?**
5 lần, vẽ trung bình và error bar (độ lệch chuẩn). Một lần chạy có thể may hoặc rủi (máy hàng xóm trên AWS, mạng); 5 lần cho thấy kết quả có ổn định không và khác biệt giữa hai codec có lớn hơn dao động không.

**G2. Vì sao xáo trộn thứ tự chạy?**
Nếu chạy hết gzip rồi mới đến lz4, một khoảng thời gian cluster chậm sẽ rơi trọn vào một codec. Xáo trộn (seed cố định để tái lập) rải nhiễu đều lên mọi cấu hình.

**G3. Vì sao xoá page cache trước mỗi lần chạy?**
Linux giữ file vừa đọc trong RAM. Không xoá thì lần sau đọc từ RAM, không chạm đĩa, và lợi ích của nén lưu trữ (ít byte đọc từ đĩa) biến mất. Lệnh: `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches` trên mọi node.

**G4. Lần khởi động (warm-up) để làm gì?**
Lần chạy đầu chậm hơn vì JVM khởi động, JIT chưa tối ưu, metadata HDFS chưa nằm trong bộ nhớ. Mỗi app chạy 1 lần không ghi lại trước khi đo.

**G5. Bao nhiêu lần chạy tổng cộng? Tính thế nào?**
9 cặp (codec lưu trữ, codec shuffle): E1 có 5, E2 có 4, chung `(none, off)` → 8, E3 thêm `(zstd, lz4)` → 9. × 3 app × 4 kích thước × 5 lần lặp = **540**.

**G6. Nếu bị ngắt giữa chừng thì sao?**
Chạy lại đúng lệnh. Dữ liệu đã sinh (có file `sf<N>.txt`) được bỏ qua; các lần chạy có `status = ok` trong `runs.csv` được bỏ qua. Chạy trong `tmux` trên master nên tắt laptop không ảnh hưởng.

**G7. Đo CPU thế nào? Đo đĩa và mạng thế nào?**
CPU: `executorCpuTime` cộng qua mọi stage (History Server), và %CPU từ `sar`. Đĩa: `sar -b` (khối 512 byte/giây) trên mọi node. Mạng: `sar -n DEV` (kB/giây, bỏ card `lo`). Byte shuffle và byte shuffle qua mạng: History Server.

**G8. Sao biết lần chạy dùng đúng cấu hình?**
Mỗi dòng `runs.csv` ghi cấu hình Spark **thực sự** dùng (`executor_cores`, `executor_memory`, `default_parallelism`, lấy từ endpoint `/environment` của History Server), cùng codec lưu trữ và shuffle. Byte shuffle thay đổi theo codec cũng xác nhận nén đã bật.

**G9. Speedup tính thế nào?**
(thời gian không nén − thời gian có nén) / thời gian không nén × 100, cùng app và cùng kích thước. Dương = nhanh hơn, âm = chậm hơn.

---

## H. Kết quả và 7 câu hỏi của đề

*(Câu trả lời cuối cùng phải dựa trên số liệu AWS. Dưới đây là cách lập luận và chỗ lấy số.)*

**H1. Codec nào cân bằng tốt nhất?**
Xem `fig5b` (tỉ lệ theo tốc độ) và `fig7`. Micro-benchmark ở dev: zstd-1 nén 3,0× ở 324 MB/s, gzip 3,3× ở 27 MB/s, lz4 1,9× ở 360 MB/s. Nếu số AWS tương tự: Zstd mức thấp có tỉ lệ gần gzip với tốc độ gần lz4.

**H2. Nén có luôn giúp app I/O-bound không?**
Không nhất thiết: chỉ có lợi khi thời gian I/O tiết kiệm được lớn hơn thời gian CPU thêm. Codec chậm (gzip) có thể làm chậm. Xem `speedup.csv`, ô I/O-bound của `fig1`.

**H3. Ảnh hưởng tới app CPU-bound?**
Rất ít lợi (ít I/O, shuffle ≈ 0); có thể chậm nhẹ vì giải nén input. Xem `fig1`/`fig2` ô CPU-bound.

**H4. Lợi ích thay đổi thế nào khi dữ liệu lớn lên?**
Thường tăng: dữ liệu lớn → I/O và shuffle chiếm tỉ trọng lớn hơn, nhiều spill hơn. Xem độ dốc trong `fig1`, cột `sf` của `speedup.csv`.

**H5. Nén lưu trữ hay nén mạng lợi hơn?**
Phụ thuộc nút thắt. Trên EC2, mạng ≥ 1,25 Gbps (~150 MB/s), đĩa gp3 125 MB/s. So hai ô của `fig7`, `fig3` (đĩa) với `fig4` (mạng), và `shuffle_fetch_wait_ms` (nếu gần 0 thì mạng không phải nút thắt).

**H6. Kết hợp hai mức có thể làm chậm không?**
Có thể: C3 chịu chi phí CPU của cả hai mức; nếu một mức không có lợi thì chỉ còn chi phí. So C3 với C1 và C2 trong `fig6`.

**H7. Khi nào nên tắt nén?**
App CPU-bound; dữ liệu không nén được; mạng/đĩa nhanh so với CPU; codec chậm cho dữ liệu đọc nhiều lần. Dẫn speedup âm và micro-benchmark trên dữ liệu ngẫu nhiên.

---

## I. Câu hỏi "khó"

**I1. Kết quả của bạn có tổng quát không?**
Đúng cho cấu hình này: m5.xlarge, mạng EC2, file text, Spark 3.5. Máy nhiều core hơn (nén song song nhiều hơn) hoặc mạng chậm hơn có thể cho kết luận khác. Phương pháp và code thì dùng lại được: đổi máy chỉ cần sửa `variables.tf`.

**I2. Nếu mạng chậm (1 Gbps) thì sao?**
Nén shuffle sẽ có lợi hơn vì truyền dữ liệu chiếm nhiều thời gian hơn. Project không giả lập mạng chậm vì đề không yêu cầu; có thể lập luận bằng `shuffle_fetch_wait_ms` và tỉ lệ nén shuffle.

**I3. Sai số đo lớn đến đâu thì kết luận không còn chắc?**
Nếu error bar của hai codec chồng lên nhau thì không kết luận được codec nào nhanh hơn; chỉ nói "không khác biệt rõ". Đó là lý do lặp 5 lần.

**I4. Tại sao không đo nhiều mức nén (zstd 1–19)?**
Micro-benchmark có đo zstd-1/3/9 để thấy xu hướng. Trên cluster dùng mức mặc định của từng hệ thống (Spark 1, Hadoop 3) để giữ số lần chạy hợp lý (540).

**I5. Vì sao file 150 000 đơn mà không phải 1 file lớn?**
File nén không chia được; nếu 1 file lớn thì bản nén chỉ có 1 task đọc, bản `none` có nhiều task → so sánh không công bằng. Mỗi file < 1 block nên mọi codec có cùng số task.

**I6. `sar` có làm chậm hệ thống không?**
Rất ít: 1 mẫu mỗi giây, ghi file nhị phân nhỏ. Và nó chạy giống nhau ở mọi lần đo, nên không làm lệch so sánh. Dịch vụ sysstat định kỳ đã được tắt để không có thêm tiến trình nền.

**I7. Vì sao driver ¼ RAM, executor ½ RAM?**
Một executor mỗi worker dùng hết core; ½ RAM cho heap executor để chừa chỗ cho phần Spark dùng thêm ngoài heap (~10%), DataNode và Linux (bộ nhớ đệm đĩa). Driver không gom dữ liệu nên ¼ RAM là đủ. Quan trọng nhất: các giá trị **giống nhau ở mọi lần chạy**.

**I8. Bạn có dùng công cụ AI không?**
Trả lời trung thực, theo quy định của môn. Ví dụ: "Có, tôi dùng AI hỗ trợ viết một phần code và tài liệu; tôi đã kiểm tra, chạy và hiểu từng phần, và tự chạy thí nghiệm, phân tích kết quả." Nếu môn có quy định khai báo, ghi rõ trong báo cáo phần nào có AI hỗ trợ.

---

## J. Câu hỏi về code (thầy có thể chỉ vào một file)

**J1. `Generator.java`: vì sao `flatMap(... .iterator())`?**
Để sinh dòng dần dần (lazy): Spark kéo từng dòng từ iterator và ghi ra file, không giữ 150 000 đơn (~85 MB) trong bộ nhớ một lúc.

**J2. `Codecs.save`: vì sao xoá output trước khi ghi?**
`saveAsTextFile` báo lỗi nếu thư mục đã tồn tại. Xoá trước để chạy lại được.

**J3. `run_experiments.py`: làm sao lấy được số đo của đúng lần chạy?**
App in `APP_ID=app-…` ra màn hình; script đọc dòng này rồi hỏi History Server `/api/v1/applications/<id>` và `/stages`. Script chờ (tối đa 180 giây) cho đến khi History Server đọc xong event log của app.

**J4. `run_experiments.py`: `run_id` dùng để làm gì?**
Định danh mỗi lần chạy (`balanced_sf5_none_lz4_r2`), dùng để bỏ qua lần đã xong khi chạy lại, đặt tên output trên HDFS và thư mục `sar`.

**J5. `cluster.yml`: vì sao cần SSH không mật khẩu từ master?**
`start-dfs.sh` và `start-workers.sh` SSH từ master sang từng worker để khởi động DataNode và Worker; `run_experiments.py` SSH sang từng node để xoá cache và chạy `sar`.

**J6. `cluster.yml`: vì sao tắt cập nhật tự động?**
`unattended-upgrades` có thể chạy giữa lúc đo, chiếm CPU, đĩa, mạng → nhiễu.

**J7. `main.tf`: vì sao ghim cùng availability zone?**
Để mạng giữa các máy giống nhau ở mọi lần chạy, và không mất phí truyền dữ liệu giữa các zone.

**J8. `Makefile`: `make run ENV=aws` làm gì?**
SSH vào master (IP lấy từ `terraform output`), mở phiên `tmux` tên `exp`, chạy `bench/run_all.sh aws` và ghi log vào `results/aws/run.log`.
