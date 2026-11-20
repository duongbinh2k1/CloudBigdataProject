# Chạy thí nghiệm trên AWS

Các lệnh đầy đủ ở [README](../README.md#aws-cluster). File này là danh sách việc cần làm và các điểm cần kiểm tra.

## 1. Chuẩn bị (một lần)

- [ ] **Budget Alert** trong AWS Billing (ví dụ 50 USD) để được báo khi chi phí vượt mức.
- [ ] **Quota vCPU**: 4 × m5.xlarge = 16 vCPU. Kiểm tra *Service Quotas → EC2 → Running On-Demand Standard instances* ở vùng eu-west-3. Tài khoản mới thường chỉ có 5 hoặc 8 vCPU → xin tăng sớm (có thể mất 1–2 ngày).
- [ ] `aws configure` (access key của một user IAM, vùng `eu-west-3`).
- [ ] Cài công cụ trên laptop: `brew install terraform ansible` (gói `ansible` có sẵn collection `ansible.posix` mà playbook cần).
- [ ] Khoá SSH: `ssh-keygen -t ed25519 -f ~/.ssh/cloud-bigdata-project`.
- [ ] `infra/terraform/terraform.tfvars` với IP của mình (`curl -s https://checkip.amazonaws.com`, thêm `/32`). Nếu IP nhà thay đổi, sửa file này rồi `terraform apply` lại.

## 2. Dựng cluster

1. `terraform init && terraform apply` → 4 máy cùng một availability zone, tạo `infra/ansible/inventory.ini`.
2. `ansible-playbook cluster.yml` (trong `infra/ansible/`). Playbook dừng lại nếu `hadoop checknative` không báo `zstd: true`.
3. Kiểm tra trên master:
   - `hdfs dfsadmin -report` → 3 DataNode;
   - http://localhost:8080 (qua tunnel SSH) → 3 worker;
   - **zstd lưu trữ chạy được với Spark**:
     ```bash
     spark-submit --class compression.datagen.Generator java/build/cloud-bigdata-project.jar 0.01 hdfs://master:9000/smoke zstd
     spark-submit --class compression.apps.IoBound java/build/cloud-bigdata-project.jar \
         hdfs://master:9000/smoke/sf0.01/zstd/lineitem hdfs://master:9000/smoke/out zstd
     hdfs dfs -ls hdfs://master:9000/smoke/out      # part-*.zst
     ```
     Nếu lỗi `native zStandard library not available`: kiểm tra `spark.*.extraLibraryPath` trong `/opt/spark/conf/spark-defaults.conf`. Nếu vẫn lỗi thì bỏ `zstd` khỏi `storage_codecs` (đề ghi *"where supported"*) và nêu trong báo cáo.

## 3. Chạy

1. Chạy toàn bộ: `cd /opt/work && tmux new -s exp "bash bench/run_all.sh aws"` (cùng script với `make run` ở local). Script lần lượt build jar, sinh dữ liệu SF 1, 5, 10, 20 (5 codec), chạy micro-benchmark, rồi 540 lần thí nghiệm.
2. Sau vài lần đo đầu tiên, xem `results/aws/aws/runs.csv`: cột `status` = `ok`, `disk_read_bytes` > 0 (xoá cache có tác dụng). Nếu sai, dừng lại (`tmux attach -t exp`, `Ctrl-c`) và sửa trước khi để chạy tiếp.
3. Xem tiến độ: `tmux attach -t exp` (thoát mà không dừng: `Ctrl-b` rồi `d`), hoặc `grep -c ',ok,' results/aws/aws/runs.csv`. Nếu bị ngắt, chạy lại đúng lệnh ở bước 1: bước nào xong rồi thì được bỏ qua.
4. (Tuỳ chọn) Sau khi xem kết quả E1/E2, đổi `combined` trong `experiments-aws.yaml` rồi chạy lại: chỉ các cấu hình mới được chạy thêm.

## 4. Kết thúc

- [ ] `rsync` kéo `results/aws/` về laptop (lệnh trong README) và **kiểm tra file đã về đủ** trước khi xoá cluster.
- [ ] `terraform destroy` → xoá máy, ổ đĩa, security group, key pair.
- [ ] Kiểm tra trong console EC2 không còn máy hay volume nào (vùng eu-west-3).
- [ ] Commit `results/aws/`.

## Chi phí (ước tính, kiểm tra lại trên trang giá của AWS)

| Mục | Giá gần đúng (eu-west-3) | 4 máy |
|---|---|---|
| m5.xlarge | ~0,22 USD/giờ | ~0,90 USD/giờ |
| gp3 200 GB | ~0,09 USD/GB/tháng | ~0,10 USD/giờ |
| **Tổng** | | **~1 USD/giờ** khi cluster đang bật |

540 lần chạy, từ vài chục giây (SF 1) đến vài phút (SF 20), cộng thời gian sinh dữ liệu → dự kiến **khoảng 1 ngày** cluster bật, tức **~25–30 USD**. Cluster bị tính tiền cả khi không chạy gì, nên `terraform destroy` ngay khi xong.
