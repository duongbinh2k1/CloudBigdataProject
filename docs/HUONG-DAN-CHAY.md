# Hướng dẫn chạy project từ đầu

Từ một máy Mac chưa có gì đến khi có đồ thị. Gồm: cài công cụ (mục 1), lấy code (mục 2), chạy local (mục 3), chuẩn bị tài khoản AWS (mục 4), chạy trên AWS (mục 5), xử lý lỗi (mục 6), dọn dẹp (mục 7).

Mọi lệnh gõ trong **Terminal của laptop**, trong thư mục project, trừ khi ghi khác.

Đã chạy được với: macOS 14 (Apple Silicon), Podman 5.5, Terraform 1.17, Ansible core 2.22, AWS CLI 2.41.

---

## 1. Cài công cụ (một lần)

Cần [Homebrew](https://brew.sh). Rồi:

```bash
brew install podman docker-compose terraform ansible awscli
```

| Công cụ | Dùng cho |
|---|---|
| `podman`, `docker-compose` | cluster local bằng container (`podman compose` dùng `docker-compose` để đọc `dev/compose.yml`) |
| `terraform` | tạo máy EC2 |
| `ansible` | cài Hadoop/Spark lên máy EC2 (gói này có sẵn collection `ansible.posix` mà playbook cần) |
| `awscli` | lệnh `aws configure` (khai báo khoá truy cập AWS) |

Python 3 có sẵn trên macOS (`/usr/bin/python3`); `make analysis` tự tạo môi trường Python riêng (`.venv`) lần đầu.

**Máy ảo Podman.** Trên Mac, container chạy trong một máy ảo Linux. Tạo một lần, cho 4 CPU và 6 GB RAM (3 container Hadoop/Spark cần chừng đó):

```bash
podman machine init --cpus 4 --memory 6144
podman machine start
```

Mỗi lần khởi động lại Mac, chạy lại `podman machine start` trước khi dùng cluster local. Kiểm tra: `podman info` không báo lỗi.

---

## 2. Lấy code

```bash
git clone git@github.com:duongbinh2k1/CloudBigdataProject.git
cd CloudBigdataProject
```

---

## 3. Chạy ở local (thử trước khi lên cloud)

```bash
make up
```
- Lần đầu: build image (Ubuntu + `cluster/install.sh`, tải Hadoop và Spark như trên EC2). Các lần sau dùng lại image nên chỉ vài giây.
- Sau đó master tự khởi động cluster bằng `cluster/start-cluster.sh`. **Chờ khoảng 1 phút.**
- Kiểm tra: mở http://localhost:8080 → thấy **2 workers**; http://localhost:9870 → tab *Datanodes* thấy **2 DataNode**.

```bash
make run
```
Chạy toàn bộ (`bench/run_all.sh dev`) ngay trong terminal, khoảng 15 phút. Các dòng sẽ thấy:
```
built java/build/cloud-bigdata-project.jar
written hdfs://master:9000/data/sf0.1/none        … gzip, snappy, lz4
csv lz4 done … random gzip-6 done                  (micro-benchmark)
0 runs already done, 24 to run
warm-up io_bound done …
[1/24] balanced_sf0.1_none_off_r1: ok in 11.5 s
…
[24/24] …: ok in … s
```
Dòng `warning: could not drop the page cache` là **bình thường** ở local (container không được phép xoá cache).

```bash
make status        # runs ok: 24
make analysis      # đồ thị + bảng → analysis/figures/dev/
make down          # tắt container (dữ liệu HDFS vẫn giữ cho lần sau)
```

Kết quả: `results/dev/` (số liệu thô), `analysis/figures/dev/` (đồ thị). Số liệu local **không dùng cho báo cáo**: chỉ để kiểm tra mọi thứ chạy đúng.

---

## 4. Chuẩn bị tài khoản AWS (một lần)

### 4.1 Bảo vệ tài khoản và đặt cảnh báo chi phí

Đăng nhập AWS Console bằng tài khoản gốc (*root*, email đăng ký):

1. **MFA cho root**: góc trên phải → *Security credentials* → *Assign MFA device* (ứng dụng xác thực trên điện thoại).
2. **Budget** (cảnh báo chi phí): tìm **Budgets** → *Create budget* → *Use a template* → **Monthly cost budget** → số tiền ví dụ `30` USD → email của bạn → *Create budget*. Budget chỉ gửi email, **không tự dừng máy**.

### 4.2 Tạo user IAM để dùng trên laptop

Không dùng tài khoản root cho công việc hằng ngày.

1. Console → **IAM** → *Users* → *Create user*.
2. *User name*: ví dụ `binhdt`. (Tuỳ chọn) tick *Provide user access to the AWS Management Console*.
3. *Set permissions* → chọn **Attach policies directly** → tìm `AdministratorAccess` → tick → *Next* → *Create user*.
4. Mở user vừa tạo → tab *Security credentials* → **Create access key** → chọn **Command Line Interface (CLI)** → tick xác nhận → *Create access key*.
5. **Chép lại** *Access key ID* và *Secret access key* (hoặc *Download .csv*). Secret **chỉ hiện một lần**.
6. Bật MFA cho user này (cùng tab *Security credentials* → *Assign MFA device*).

### 4.3 `aws configure`

```bash
aws configure
```
```
AWS Access Key ID [None]: <dán Access key ID>
AWS Secret Access Key [None]: <dán Secret access key>
Default region name [None]: ap-southeast-2
Default output format [None]: json
```
- Câu hỏi *"Configure AWS skills and the AWS MCP server for your AI coding agent(s)?"* (bản CLI mới): chọn `n`.
- Khoá được lưu ở `~/.aws/credentials` trên laptop (không nằm trong project, không lên git).

Kiểm tra:
```bash
aws sts get-caller-identity
```
Phải thấy `"Arn": "arn:aws:iam::<số tài khoản>:user/binhdt"`.

### 4.4 Kiểm tra giới hạn của tài khoản

Tài khoản **Free plan** đang dùng có ba giới hạn:

| Giới hạn | Giá trị | Ảnh hưởng |
|---|---|---|
| Vùng | chỉ **Sydney** (`ap-southeast-2`) | `infra/terraform/variables.tf` đã đặt sẵn |
| Loại máy | chỉ các máy Free plan; dùng được **`m7i-flex.large`** (2 vCPU, 8 GB) | đã đặt sẵn |
| Quota vCPU | **16** → tối đa 8 máy `m7i-flex.large` | `worker_count` ≤ 7 |

Kiểm tra quota (phải ra `16.0` hoặc lớn hơn):
```bash
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --region ap-southeast-2 --query Quota.Value
```

---

## 5. Chạy trên AWS

### 5.1 Chọn cấu hình

Hai file quyết định đợt đo:

| File | Dòng | Đợt đo chính thức (hiện tại) | Thử nghiệm nhỏ (nếu muốn thử trước) |
|---|---|---|---|
| `infra/terraform/variables.tf` | `worker_count` | `7` | `3` |
| `bench/experiments-aws.yaml` | `sizes` | `[1, 5, 10, 20]` | `[1]` |
| `bench/experiments-aws.yaml` | `repetitions` | `3` | `1` |

Hiện đặt sẵn đợt chính thức: 324 lần chạy, khoảng 18–24 giờ, khoảng 20–30 USD credit (chi tiết: [AWS.md](AWS.md)). Thử nghiệm nhỏ đã chạy thành công một lần (`results/aws-test/`); muốn thử lại thì đổi sang cột bên phải (khoảng 1–2 giờ).

### 5.2 Dựng cluster

```bash
make up ENV=aws
```
1. Tự tạo khoá SSH `~/.ssh/cloud-bigdata-project` (nếu chưa có).
2. Terraform in ra danh sách tài nguyên sẽ tạo (VPC, subnet, security group, các máy…) rồi hỏi:
   ```
   Do you want to perform these actions?
     Enter a value: yes
   ```
   Gõ `yes`. **Từ đây bắt đầu tính tiền** (trừ vào credit).
3. Ansible cài và khởi động cluster (~10 phút). Thành công khi cuối cùng thấy `PLAY RECAP` với `failed=0` ở mọi máy.

Kiểm tra (tuỳ chọn):
```bash
make shell ENV=aws          # SSH vào master, đồng thời mở web UI trên laptop
hdfs dfsadmin -report | grep "Live datanodes"      # trên master: số DataNode
exit
```
Trong lúc `make shell ENV=aws` còn mở: http://localhost:8080 (Spark), http://localhost:9870 (HDFS), http://localhost:18080 (History Server).

### 5.3 Chạy thí nghiệm

```bash
make run ENV=aws
```
Trả về ngay (`started on the master…`): thí nghiệm chạy trong **tmux** trên master, **tắt laptop vẫn chạy**.

Theo dõi (gõ lại vài lần):
```bash
make status ENV=aws
```
```
exp: 1 windows (created …)            ← còn đang chạy ("not running" = đã xong hoặc dừng)
written hdfs://master:9000/data/sf1/zstd
…
[12/27] cpu_bound_sf1_zstd_off_r1: ok in 68.4 s
runs ok: 12
```

Kiểm tra sớm, sau khoảng 5 lần đo:
```bash
make fetch ENV=aws
```
Mở `results/aws/runs.csv`: cột `status` phải là `ok`, cột `disk_read_bytes` > 0. Nếu có `failed`: xem `results/aws/logs/<lần đo>.log`.

Xem trực tiếp màn hình đang chạy:
```bash
make shell ENV=aws
tmux attach -t exp        # thoát mà KHÔNG dừng: Ctrl-b rồi d
```

### 5.4 Khi xong

```bash
make status ENV=aws       # "not running" và runs ok = tổng số lần đo
make fetch ENV=aws        # kéo results/aws về laptop
make analysis ENV=aws     # → analysis/figures/aws/
make down ENV=aws         # xoá cluster; gõ yes
```
- **Luôn `fetch` trước `down`**: `down` xoá cả ổ đĩa của các máy.
- Sau `down`, mở EC2 Console, vùng **Sydney**: không còn *Instances* và *Volumes* nào.
- Commit kết quả: `git add results/aws && git commit -m "results: experiments on EC2"`.

---

## 6. Xử lý lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách làm |
|---|---|---|
| `Cannot connect to Podman` | máy ảo Podman chưa chạy | `podman machine start` |
| Không có worker trên http://localhost:8080 | cluster chưa khởi động xong | chờ 1–2 phút; xem log: `podman logs cloud-bigdata-dev-master-1` |
| `no EC2 cluster: run make up ENV=aws first` | chưa có cluster (hoặc đã `down`) | `make up ENV=aws` |
| `make status/shell ENV=aws` treo hoặc *Connection timed out* | IP nhà đã đổi, security group chỉ cho IP cũ | `make up ENV=aws` (cập nhật IP, không tạo lại máy) |
| Terraform: `UnauthorizedOperation` … `service control policy` | vùng hoặc loại máy bị Free plan chặn | dùng `ap-southeast-2` và `m7i-flex.large` |
| Terraform: `VcpuLimitExceeded` | vượt quota vCPU | giảm `worker_count` |
| Terraform: `InvalidClientTokenId` / `ExpiredToken` | khoá AWS sai hoặc đã xoá | tạo access key mới, `aws configure` lại |
| Ansible: `UNREACHABLE` | máy chưa khởi động xong, hoặc IP đổi | chạy lại `make up ENV=aws` |
| `rounds changed since the runs in …` | đã đổi `rounds` trong YAML | chuyển `results/<env>` sang chỗ khác rồi chạy lại |
| Bị ngắt giữa chừng (mất điện, mất mạng) | | chạy lại `make run` / `make run ENV=aws`: phần đã xong được bỏ qua |
| Sửa code/YAML khi cluster AWS đang chạy | code trên master là bản cũ | `make up ENV=aws` (copy code mới, build lại jar), rồi `make run ENV=aws` |

---

## 7. Dọn dẹp

```bash
make down ENV=aws                # xoá mọi thứ trên AWS (quan trọng nhất: hết tốn tiền)
make down                        # tắt cluster local
podman volume rm cloud-bigdata-dev_master-data cloud-bigdata-dev_worker1-data cloud-bigdata-dev_worker2-data   # (tuỳ chọn) xoá dữ liệu HDFS local
podman machine stop              # (tuỳ chọn) tắt máy ảo Podman
```

Sau khi nộp bài: xoá access key của user IAM (IAM → Users → `binhdt` → *Security credentials* → *Deactivate* rồi *Delete*), và các user cấp cho người khác xem.

**Đừng xoá `infra/terraform/terraform.tfstate`** khi cluster AWS còn tồn tại: Terraform cần file này để biết phải xoá những gì.
