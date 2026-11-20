# Chạy thí nghiệm trên AWS

Mọi lệnh chạy trên laptop, trong thư mục project (bảng lệnh ở [README](../README.md#how-to-run)). File này là danh sách việc cần làm và các điểm cần kiểm tra.

## 1. Chuẩn bị (một lần)

- [ ] **Budget Alert** trong AWS Billing (ví dụ 50 USD) để được báo khi chi phí vượt mức.
- [ ] **Quota vCPU**: 4 × m5.xlarge = 16 vCPU. Kiểm tra *Service Quotas → EC2 → Running On-Demand Standard instances* ở vùng eu-west-3. Tài khoản mới thường chỉ có 5 hoặc 8 vCPU → xin tăng sớm (có thể mất 1–2 ngày).
- [ ] `aws configure` (access key của một user IAM, vùng `eu-west-3`).
- [ ] Cài công cụ trên laptop: `brew install terraform ansible` (gói `ansible` có sẵn collection `ansible.posix` mà playbook cần).

Khoá SSH (`~/.ssh/cloud-bigdata-project`) được `make up ENV=aws` tự tạo nếu chưa có. Chỉ IP hiện tại của laptop được phép SSH vào cluster (lấy tự động mỗi lần `make up`/`make down`); nếu IP nhà thay đổi, chạy lại `make up ENV=aws`.

## 2. Dựng cluster

1. `make up ENV=aws`: Terraform tạo 4 máy trong cùng một availability zone, rồi Ansible cài và khởi động HDFS + Spark, copy `java/` + `bench/` lên `/opt/work` và build jar. Playbook dừng lại nếu `hadoop checknative` không báo `zstd: true`.
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
- [ ] Kiểm tra trong console EC2 không còn máy hay volume nào (vùng eu-west-3).
- [ ] `make analysis ENV=aws`, rồi commit `results/aws/`.

## Đổi cấu hình

| Muốn đổi | Sửa | Rồi chạy |
|---|---|---|
| Loại máy, số worker, dung lượng đĩa, vùng | `infra/terraform/variables.tf` | `make up ENV=aws` |
| Kích thước dữ liệu, codec, số lần lặp, ứng dụng, cặp codec E3 | `bench/experiments-aws.yaml` | `make up ENV=aws` (copy file lên), `make run ENV=aws` |
| Độ nặng CPU của app (`rounds`) | `bench/experiments-aws.yaml` | như trên, nhưng trước đó chuyển `results/aws` đi chỗ khác: script từ chối trộn kết quả của hai workload khác nhau |

Không phải sửa gì khác: Ansible tự tính core, RAM của executor, `parallelism` (số worker × core × 4) và số bản sao HDFS từ máy thật; danh sách máy cho `sar` lấy từ danh sách worker của Spark. Cấu hình Spark thực tế của mỗi lần chạy được ghi trong `runs.csv` (`executor_cores`, `executor_memory`, `default_parallelism`).

Thêm kích thước, codec hay số lần lặp thì chỉ các lần chạy mới được chạy thêm; các lần đã `ok` được giữ lại.

## Chi phí (ước tính, kiểm tra lại trên trang giá của AWS)

| Mục | Giá gần đúng (eu-west-3) | 4 máy |
|---|---|---|
| m5.xlarge | ~0,22 USD/giờ | ~0,90 USD/giờ |
| gp3 200 GB | ~0,09 USD/GB/tháng | ~0,10 USD/giờ |
| **Tổng** | | **~1 USD/giờ** khi cluster đang bật |

540 lần chạy, từ vài chục giây (SF 1) đến vài phút (SF 20), cộng thời gian sinh dữ liệu → dự kiến **khoảng 1 ngày** cluster bật, tức **~25–30 USD**. Cluster bị tính tiền cả khi không chạy gì, nên `make down ENV=aws` ngay khi xong.
