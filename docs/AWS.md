# Chạy thí nghiệm trên AWS

Mọi lệnh chạy trên laptop, trong thư mục project (bảng lệnh ở [README](../README.md#how-to-run)). File này là danh sách việc cần làm và các điểm cần kiểm tra.

## 1. Chuẩn bị (một lần)

- [ ] **Budget Alert** trong AWS Billing (ví dụ 50 USD) để được báo khi chi phí vượt mức.
- [ ] **Vùng được phép**: tài khoản Free plan này chỉ được dùng **Sydney (`ap-southeast-2`)**; các vùng khác (kể cả Singapore) bị một chính sách của AWS Organization chặn. Terraform đang dùng Sydney. Từ Việt Nam đến Sydney xa hơn nhưng không ảnh hưởng số đo (số đo là mạng giữa các máy trong cluster, cùng một zone). Nếu sau khi nâng lên Paid plan Singapore được mở, đổi `region` trong `infra/terraform/variables.tf`.
- [ ] **Quota vCPU** ở Sydney: *Service Quotas → EC2 → Running On-Demand Standard instances*. Hiện là **16 vCPU**: đủ cho cluster test (4 × m7i-flex.large = 8 vCPU) và cluster chính thức (4 × m5.xlarge = 16 vCPU).
- [ ] **Gói tài khoản**: Free plan chỉ cho một số loại máy (có `m7i-flex.large`, không có `m5.xlarge`). Kiểm tra trong *EC2 → Instance Types*. Để chạy số liệu chính thức với `m5.xlarge`, chuyển lên Paid plan: credit còn lại vẫn được dùng, chỉ tính tiền vào thẻ khi hết credit.
- [ ] `aws configure` (access key của một user IAM có `AdministratorAccess`; vùng `ap-southeast-2` cho khớp với Terraform).
- [ ] Cài công cụ trên laptop: `brew install terraform ansible` (gói `ansible` có sẵn collection `ansible.posix` mà playbook cần).

Khoá SSH (`~/.ssh/cloud-bigdata-project`) được `make up ENV=aws` tự tạo nếu chưa có. Chỉ IP hiện tại của laptop được phép SSH vào cluster (lấy tự động mỗi lần `make up`/`make down`); nếu IP nhà thay đổi, chạy lại `make up ENV=aws`.

## 2. Dựng cluster

1. `make up ENV=aws`: Terraform tạo 4 máy trong cùng một availability zone, rồi Ansible cài và khởi động HDFS + Spark, copy `java/` + `bench/` lên `/opt/work` và build jar.
2. (Tuỳ chọn) `make shell ENV=aws` rồi `hdfs dfsadmin -report` → 3 DataNode; http://localhost:8080 → 3 worker.

zstd không cần kiểm tra riêng: `make run ENV=aws` sinh dữ liệu zstd ngay ở bước đầu. Nếu thư viện native không chạy, script dừng ở đó và `make status ENV=aws` hiện lỗi. Khi đó kiểm tra `spark.*.extraLibraryPath` trong `/opt/spark/conf/spark-defaults.conf`; nếu vẫn lỗi thì bỏ `zstd` khỏi `storage_codecs` trong `bench/experiments-aws.yaml` (đề ghi *"where supported"*), `make up ENV=aws` để copy file mới lên, rồi `make run ENV=aws`, và nêu điều này trong báo cáo.

Sửa code sau khi cluster đã chạy: chạy lại `make up ENV=aws` (Terraform không thay đổi gì, Ansible copy lại code và build lại jar).

## 3. Chạy

1. `make run ENV=aws`: chạy `bench/run_all.sh aws` trong `tmux` trên master (cùng script với `make run` ở local). Script lần lượt build jar, sinh dữ liệu SF 1, 5, 10, 20 (5 codec), chạy micro-benchmark, rồi 540 lần thí nghiệm. Có thể tắt laptop.
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

## Hai giai đoạn: test rồi chính thức

| | Test (hiện tại) | Chính thức (số liệu báo cáo) |
|---|---|---|
| `instance_type` (`infra/terraform/variables.tf`) | `m7i-flex.large` (2 vCPU, 8 GB, có trong Free plan) | `m5.xlarge` (4 vCPU, 16 GB) |
| `sizes`, `repetitions` (`bench/experiments-aws.yaml`) | `[1]`, `1` → 27 lần chạy | `[1, 5, 10, 20]`, `5` → 540 lần chạy |

Hai chỗ đổi đều có ghi chú `TEMPORARY` trong file. Kết quả của bản test đã chạy được giữ ở `results/aws-test/` (không đưa lên git) để không trộn với kết quả chính thức trong `results/aws/`. Máy "flex" không đảm bảo luôn có 100% CPU, nên số đo trên cluster test không dùng cho báo cáo. Giữa hai giai đoạn: `make down ENV=aws`, sửa hai file, rồi `make up ENV=aws` và `make run ENV=aws` lại từ đầu (dữ liệu và kết quả trên cluster cũ bị xoá theo).

## Chi phí (ước tính, kiểm tra lại trên trang giá EC2 cho vùng Sydney)

| Mục | 4 máy, khi cluster đang bật |
|---|---|
| m5.xlarge | khoảng 1 USD/giờ |
| m7i-flex.large (test) | khoảng một nửa |
| gp3 200 GB × 4 | khoảng 0,1 USD/giờ |

Bản test (SF 1, 27 lần chạy) mất khoảng 1–2 giờ. Bản chính thức (540 lần chạy, SF 1 đến 20) dự kiến khoảng 1 ngày, tức khoảng 25–35 USD. Credit của tài khoản mới (100 USD, có thể thêm 100 USD) đủ cho cả hai. Cluster bị tính tiền cả khi không chạy gì, nên `make down ENV=aws` ngay khi xong.
