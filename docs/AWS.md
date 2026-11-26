# Chạy thí nghiệm trên AWS

Hướng dẫn đầy đủ từng bước (cài công cụ, tạo user IAM, `aws configure`…): [HUONG-DAN-CHAY.md](HUONG-DAN-CHAY.md). Mọi lệnh chạy trên laptop, trong thư mục project (bảng lệnh ở [README](../README.md#how-to-run)). File này là danh sách việc cần làm và các điểm cần kiểm tra.

## 1. Chuẩn bị (một lần)

- [ ] **Budget Alert** trong AWS Billing (ví dụ 50 USD) để được báo khi chi phí vượt mức.
- [ ] **Vùng được phép**: tài khoản Free plan này chỉ được dùng **Sydney (`ap-southeast-2`)**; các vùng khác (kể cả Singapore) bị một chính sách của AWS Organization chặn. Terraform đang dùng Sydney. Từ Việt Nam đến Sydney xa hơn nhưng không ảnh hưởng số đo (số đo là mạng giữa các máy trong cluster, cùng một zone). Nếu sau khi nâng lên Paid plan Singapore được mở, đổi `region` trong `infra/terraform/variables.tf`.
- [ ] **Quota vCPU** ở Sydney: *Service Quotas → EC2 → Running On-Demand Standard instances*. Hiện là **16 vCPU** = đúng 8 máy `m7i-flex.large` (1 master + 7 worker).
- [ ] **Gói tài khoản**: Free plan chỉ cho một số loại máy (có `m7i-flex.large`, không có `m5`). Kiểm tra trong *EC2 → Instance Types*.
- [ ] `aws configure` (access key của một user IAM có `AdministratorAccess`; vùng `ap-southeast-2` cho khớp với Terraform).
- [ ] Cài công cụ trên laptop: `brew install terraform ansible` (gói `ansible` có sẵn collection `ansible.posix` mà playbook cần).

Khoá SSH (`~/.ssh/cloud-bigdata-project`) được `make up ENV=aws` tự tạo nếu chưa có. Chỉ IP hiện tại của laptop được phép SSH vào cluster (lấy tự động mỗi lần `make up`/`make down`); nếu IP nhà thay đổi, chạy lại `make up ENV=aws`.

## 2. Dựng cluster

1. `make up ENV=aws`: Terraform tạo 8 máy (1 master + 7 worker) trong cùng một availability zone, rồi Ansible cài và khởi động HDFS + Spark, copy `java/` + `bench/` lên `/opt/work` và build jar.
2. (Tuỳ chọn) `make shell ENV=aws` rồi `hdfs dfsadmin -report` → 7 DataNode; http://localhost:8080 → 7 worker.

zstd không cần kiểm tra riêng: `make run ENV=aws` sinh dữ liệu zstd ngay ở bước đầu. Nếu thư viện native không chạy, script dừng ở đó và `make status ENV=aws` hiện lỗi. Khi đó kiểm tra `spark.*.extraLibraryPath` trong `/opt/spark/conf/spark-defaults.conf`; nếu vẫn lỗi thì bỏ `zstd` khỏi `storage_codecs` trong `bench/experiments-aws.yaml` (đề ghi *"where supported"*), `make up ENV=aws` để copy file mới lên, rồi `make run ENV=aws`, và nêu điều này trong báo cáo.

Sửa code sau khi cluster đã chạy: chạy lại `make up ENV=aws` (Terraform không thay đổi gì, Ansible copy lại code và build lại jar).

## 3. Chạy

1. `make run ENV=aws`: chạy `bench/run_all.sh aws` trong `tmux` trên master (cùng script với `make run` ở local). Script lần lượt build jar, sinh dữ liệu SF 1, 5, 10, 20 (5 codec), chạy micro-benchmark, rồi 324 lần thí nghiệm. Có thể tắt laptop.
2. Sau vài lần đo đầu tiên, `make fetch ENV=aws` rồi xem `results/aws/runs.csv`: cột `status` = `ok`, `disk_read_bytes` > 0 (xoá cache có tác dụng). Nếu sai, dừng lại (`make shell ENV=aws`, `tmux attach -t exp`, `Ctrl-c`) và sửa trước khi để chạy tiếp.
3. Xem tiến độ: `make status ENV=aws` (phiên tmux, 5 dòng log cuối, số lần chạy `ok`). Nếu bị ngắt, chạy lại `make run ENV=aws`: bước nào xong rồi thì được bỏ qua.
4. (Tuỳ chọn) Sau khi xem kết quả E1/E2, đổi `combined` trong `experiments-aws.yaml`, `make up ENV=aws` (copy file mới lên), rồi `make run ENV=aws`: chỉ các cấu hình mới được chạy thêm.

## 4. Kết thúc

- [ ] `make fetch ENV=aws` kéo `results/aws/` về laptop và **kiểm tra file đã về đủ** (số dòng `ok` trong `runs.csv`) trước khi xoá cluster.
- [ ] `make down ENV=aws` → xoá máy, ổ đĩa, security group, key pair (Terraform hỏi xác nhận).
- [ ] Kiểm tra trong console EC2 không còn máy hay volume nào (vùng Sydney).
- [ ] `make analysis ENV=aws`, rồi commit `results/aws/`.

## Đổi cấu hình

| Muốn đổi | Sửa | Rồi chạy |
|---|---|---|
| Loại máy, số worker, dung lượng đĩa, vùng | `infra/terraform/variables.tf` | `make up ENV=aws` |
| Kích thước dữ liệu, codec, số lần lặp, ứng dụng, cặp codec E3 | `bench/experiments-aws.yaml` | `make up ENV=aws` (copy file lên), `make run ENV=aws` |
| Độ nặng CPU của app (`rounds`) | `bench/experiments-aws.yaml` | như trên, nhưng trước đó chuyển `results/aws` đi chỗ khác: script từ chối trộn kết quả của hai workload khác nhau |

Không phải sửa gì khác: Spark dùng mặc định (hết core của mỗi worker, executor 1 GB, số partition = tổng số core), tự theo máy mới; danh sách máy cho `sar` lấy từ danh sách worker của Spark.

Thêm kích thước, codec hay số lần lặp thì chỉ các lần chạy mới được chạy thêm; các lần đã `ok` được giữ lại.

## Cấu hình chính thức (đúng yêu cầu đề)

| | Giá trị | File |
|---|---|---|
| Máy | 8 × `m7i-flex.large` (2 vCPU, 8 GB): 1 master + 7 worker | `infra/terraform/variables.tf` |
| Đĩa | gp3 100 GB mỗi máy (dữ liệu ≈ 38 GB mỗi worker) | `infra/terraform/variables.tf` |
| Kích thước dữ liệu | SF 1, 5, 10, 20 (≈ 1, 5, 10, 20 GB text, §5.4 của đề) | `bench/experiments-aws.yaml` |
| Lặp lại | 3 lần mỗi cấu hình (đủ để tính độ lệch chuẩn, vẽ error bar) | `bench/experiments-aws.yaml` |
| Số lần chạy | 9 cặp codec × 3 app × 4 kích thước × 3 lần = **324** | |

Vì sao `m7i-flex.large`: tài khoản Free plan không cho dùng `m5`, và quota 16 vCPU. Máy "flex" chạy được 100% CPU phần lớn thời gian nhưng AWS không bảo đảm luôn luôn; ảnh hưởng được giảm nhờ lặp 3 lần, thứ tự chạy xáo trộn, và error bar cho thấy độ dao động. Nêu điều này trong báo cáo.

Kết quả của lần test trước (4 máy, SF 1, 1 lần lặp) nằm ở `results/aws-test/` (không đưa lên git), tách khỏi `results/aws/`. Nếu cluster test còn bật: `make down ENV=aws` trước, rồi `make up ENV=aws` và `make run ENV=aws`.

## Chi phí và thời gian (ước tính từ lần test; kiểm tra lại trên trang giá EC2 vùng Sydney)

| Mục | Khi cluster bật |
|---|---|
| 8 × m7i-flex.large | khoảng 1 USD/giờ |
| gp3 100 GB × 8 | khoảng 0,1 USD/giờ |

Lần test (3 worker, SF 1): mỗi lần chạy 25–70 giây. Với 7 worker và tổng 36 SF cho mỗi cấu hình, đợt chính thức dự kiến **khoảng 18–24 giờ**, tức **khoảng 20–30 USD**, nằm trong 100 USD credit. Cluster bị tính tiền cả khi không chạy gì, nên `make down ENV=aws` ngay khi xong.
