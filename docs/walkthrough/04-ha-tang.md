# 04 — Hạ tầng: cài đặt, cấu hình, local, AWS, Makefile

Nguyên tắc: **EC2 là môi trường chính, local là bản mô phỏng**. Cài đặt và khởi động chỉ viết **một lần** trong `cluster/`, dùng cho cả hai. Mỗi bên chỉ thêm phần riêng của mình:

```
                    cluster/install.sh  +  cluster/start-cluster.sh  +  cluster/hadoop, cluster/spark
                         ▲                                                        ▲
        ┌────────────────┴───────────────┐                      ┌─────────────────┴──────────────┐
        │ LOCAL (dev/)                   │                      │ EC2 (infra/)                   │
        │ máy = container ubuntu:22.04   │                      │ máy = EC2 Ubuntu 22.04         │
        │ tạo máy: compose.yml           │                      │ tạo máy: Terraform             │
        │ chạy script: Dockerfile,       │                      │ chạy script: Ansible           │
        │              entrypoint.sh     │                      │                                │
        │ riêng: root, khoá SSH chung    │                      │ riêng: tên máy, /etc/hosts,    │
        │                                │                      │        tắt cập nhật, SSH mesh  │
        └────────────────────────────────┘                      └────────────────────────────────┘
```

## 1. `cluster/install.sh`: cài một máy

Chạy bằng `root`. Tham số là user sẽ chạy các daemon: `ubuntu` trên EC2, `root` (mặc định) ở local.

```bash
set -e                    # một lệnh lỗi → dừng cả script (không cài dở dang mà tưởng là xong)
user=${1:-root}           # tham số 1, không có thì "root"
cd "$(dirname "$0")"      # vào thư mục chứa script (để thấy hadoop/, spark/)
```

```bash
export DEBIAN_FRONTEND=noninteractive     # apt không dừng lại hỏi (múi giờ…)
apt-get update                            # cập nhật danh sách gói
apt-get install -y openjdk-11-jdk-headless python3-yaml sysstat libzstd1 openssh-server sudo curl tmux
```

| Gói | Để làm gì |
|---|---|
| `openjdk-11-jdk-headless` | Java 11 (Hadoop, Spark chạy trên Java); JDK vì cần `javac`, `jar` để build và để chạy `CodecBench.java` |
| `python3-yaml` | `run_experiments.py` đọc file YAML (Python 3 có sẵn trong Ubuntu) |
| `sysstat` | lệnh `sar`, `sadf` đo CPU, đĩa, mạng |
| `libzstd1` | thư viện zstd mà codec zstd của Hadoop gọi tới |
| `openssh-server` | SSH vào máy (EC2 có sẵn; container thì chưa) |
| `sudo` | lệnh xoá page cache dùng `sudo` |
| `curl` | tải Hadoop/Spark |
| `tmux` | giữ thí nghiệm chạy khi ngắt SSH |

```bash
[ -d /opt/hadoop-3.4.1 ] || curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz | tar -xz -C /opt
[ -d /opt/spark-3.5.5-bin-hadoop3 ] || curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -xz -C /opt
```
- `[ -d X ] || lệnh`: nếu thư mục X **đã có** thì bỏ qua, chưa có thì chạy lệnh. Chạy `make up ENV=aws` lần hai (để cập nhật code) không tải lại 1 GB.
- `curl -fsSL`: tải; `-f` lỗi HTTP thì báo lỗi, `-s` im lặng, `-S` vẫn in lỗi, `-L` theo chuyển hướng.
- `| tar -xz -C /opt`: giải nén ngay trong lúc tải (`-x` giải nén, `-z` gzip, `-C /opt` vào `/opt`).
- `hadoop-3.4.1-lean`: bản Hadoop "gọn" (bỏ thư viện AWS SDK không cần). `spark-3.5.5-bin-hadoop3`: Spark build sẵn cho Hadoop 3.

```bash
ln -sfn /opt/hadoop-3.4.1 /opt/hadoop            # đường dẫn ngắn, không phụ thuộc phiên bản
ln -sfn /opt/spark-3.5.5-bin-hadoop3 /opt/spark
```

```bash
cat > /etc/profile.d/bigdata.sh <<'END'
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=/opt/spark/bin:/opt/spark/sbin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
END
```
- Mọi *login shell* (`bash -l`, đăng nhập SSH) đọc `/etc/profile.d/*.sh` → có các biến này, gõ `hdfs`, `spark-submit`, `start-dfs.sh` ở đâu cũng được (giống slide: "define environment variables").
- Đó là lý do khắp nơi dùng `bash -l`: Makefile, `entrypoint.sh`, Ansible.

```bash
cp hadoop/* /opt/hadoop/etc/hadoop/      # cấu hình Hadoop (mục 3)
cp spark/* /opt/spark/conf/              # cấu hình Spark
mkdir -p /data/hdfs /opt/work            # dữ liệu HDFS ; thư mục project
chown -R "$user" /data/hdfs /opt/work /opt/hadoop-3.4.1 /opt/spark-3.5.5-bin-hadoop3
```
`chown`: user chạy daemon phải ghi được vào thư mục dữ liệu, thư mục log (`/opt/hadoop/logs`, `/opt/spark/logs`) và thư mục làm việc của Spark (`/opt/spark/work`).

## 2. `cluster/start-cluster.sh`: khởi động cả cluster từ master

Chạy trên **master**, bằng login shell (`bash -l`) để có `PATH`.

```bash
[ -d /data/hdfs/namenode/current ] || hdfs namenode -format -nonInteractive
```
**Format NameNode** chỉ lần đầu (tạo "mục lục" rỗng). Format lại là xoá sạch HDFS, nên phải có điều kiện.

```bash
stop-dfs.sh || true      # nếu đang chạy thì dừng (|| true: không chạy cũng không coi là lỗi)
start-dfs.sh
```
`start-dfs.sh` (slide Hadoop trang 25):
1. bật **NameNode** trên máy này;
2. đọc `/opt/hadoop/etc/hadoop/workers`, **SSH vào từng worker**, bật **DataNode**;
3. bật **SecondaryNameNode** (SSH tới `0.0.0.0`, lý do slide nói phải SSH không hỏi "kể cả 0.0.0.0").

```bash
hdfs dfsadmin -safemode wait     # chờ HDFS hết "safe mode" (đủ DataNode báo cáo) mới được ghi
hdfs dfs -mkdir -p /spark-logs /data /results
```
- `/spark-logs`: event log của Spark (History Server đọc). `/data`: dữ liệu sinh ra. `/results`: output tạm của các app.

```bash
stop-history-server.sh || true
stop-workers.sh || true
stop-master.sh || true
start-master.sh          # Spark Master trên máy này, cổng 7077 (web 8080)
start-workers.sh         # SSH vào từng máy trong /opt/spark/conf/workers, bật Spark Worker (slide Spark trang 29)
start-history-server.sh  # History Server, web + API cổng 18080
```
Dừng trước rồi mới bật: chạy lại script thì cluster được khởi động lại sạch sẽ.

## 3. File cấu hình (`cluster/hadoop/`, `cluster/spark/`)

Chỉ có những dòng **cần để chạy**; mọi thứ khác là mặc định.

**`hadoop/core-site.xml`**
```xml
<property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
```
Địa chỉ NameNode. Mọi máy và mọi lệnh `hdfs` dùng nó để biết "HDFS ở đâu".

**`hadoop/hdfs-site.xml`**
```xml
<property><name>dfs.namenode.name.dir</name><value>file:///data/hdfs/namenode</value></property>
<property><name>dfs.datanode.data.dir</name><value>file:///data/hdfs/datanode</value></property>
<property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
```
- Chỗ NameNode lưu "mục lục" và DataNode lưu block. Mặc định là `/tmp/...`, bị xoá khi khởi động lại máy.
- Dòng cuối: NameNode mặc định kiểm tra IP của DataNode có tra ngược ra tên máy được không (*reverse DNS*). Mạng container không có tra ngược nên phải tắt; trên EC2 không ảnh hưởng.
- Không đặt `dfs.replication` (mặc định 3) và `dfs.blocksize` (mặc định 128 MB).

**`hadoop/hadoop-env.sh`**
```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java))))
```
Hadoop bắt buộc có `JAVA_HOME` (slide: "JAVA_HOME defined"). Dòng này tự dò: `command -v java` → `/usr/bin/java`; `readlink -f` → `/usr/lib/jvm/java-11-openjdk-amd64/bin/java` (EC2) hoặc `…-arm64/bin/java` (Mac); bỏ `/bin/java` hai lần → thư mục Java. Nhờ vậy **cùng một file** chạy được trên cả x86 (EC2) lẫn ARM (Mac).

**`spark/spark-env.sh`**
```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java))))
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop     # Spark đọc cấu hình HDFS ở đây
export SPARK_MASTER_HOST=master                   # Master gắn vào tên "master"
```
`SPARK_MASTER_HOST`: Master lắng nghe với tên `master`, khớp với `spark://master:7077` ở mọi nơi.

**`spark/spark-defaults.conf`** (Spark đọc mỗi lần `spark-submit`, và History Server đọc khi khởi động)
```
spark.master                      spark://master:7077          # app nộp cho Master này
spark.eventLog.enabled            true                         # ghi event log …
spark.eventLog.dir                hdfs://master:9000/spark-logs   # … vào đây
spark.history.fs.logDirectory     hdfs://master:9000/spark-logs   # History Server đọc ở đây
spark.driver.extraLibraryPath     /opt/hadoop/lib/native       # chỗ tìm libhadoop.so (zstd)
spark.executor.extraLibraryPath   /opt/hadoop/lib/native
```
Không có core, RAM hay `parallelism`: Spark dùng mặc định (mỗi worker cho dùng hết core và RAM − 1 GB; mỗi app một executor mỗi worker, 1 GB heap; driver 1 GB; số partition = tổng số core).

## 4. Cluster local (`dev/`)

**`dev/Dockerfile`**: công thức tạo image (một "máy").
```dockerfile
FROM ubuntu:22.04                                       # Ubuntu trắng, như máy EC2
COPY cluster/ /opt/cluster/
RUN bash /opt/cluster/install.sh                        # CÙNG script với EC2
```
- Cài đặt giống hệt EC2: `install.sh` cài gói, tải và giải nén Hadoop/Spark, cấu hình.
- `RUN`: chạy lệnh lúc build image; kết quả được lưu trong image.

```dockerfile
RUN ssh-keygen -q -t ed25519 -N "" -f /root/.ssh/id_ed25519 \
 && cp /root/.ssh/id_ed25519.pub /root/.ssh/authorized_keys \
 && printf 'Host *\n  StrictHostKeyChecking no\n  LogLevel ERROR\n' > /root/.ssh/config
```
SSH không mật khẩu: tạo một cặp khoá, cho phép chính khoá đó đăng nhập. Mọi container dùng chung image nên có chung khoá → container nào cũng SSH được sang container khác. `StrictHostKeyChecking no`: không hỏi "có tin máy này không" lần đầu.

```dockerfile
COPY dev/workers /opt/hadoop/etc/hadoop/workers         # danh sách worker: worker1, worker2
COPY dev/workers /opt/spark/conf/workers
RUN printf 'export HDFS_NAMENODE_USER=root\n…' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
```
Trong container mọi thứ chạy bằng `root`; Hadoop 3 bắt khai báo rõ thì mới cho chạy daemon bằng root.

```dockerfile
COPY dev/entrypoint.sh /entrypoint.sh
WORKDIR /opt/work                                       # thư mục mặc định khi vào container
ENTRYPOINT ["bash", "-l", "/entrypoint.sh"]             # lệnh chạy khi container khởi động
```

**`dev/entrypoint.sh`**
```bash
service ssh start                                  # mỗi container = một máy có SSH server
if [ "$ROLE" = "master" ]; then
  for w in $(cat /opt/hadoop/etc/hadoop/workers); do
    until ssh $w true; do sleep 2; done            # đợi đến khi SSH vào được từng worker
  done
  bash -l /opt/cluster/start-cluster.sh            # CÙNG script với EC2
fi
tail -f /dev/null                                  # giữ container chạy mãi
```

**`dev/compose.yml`**
```yaml
name: cloud-bigdata-dev
services:
  master:
    build: {context: .., dockerfile: dev/Dockerfile}   # build từ thư mục gốc project
    image: cloud-bigdata-project/node:dev
    hostname: master                                   # tên máy → các máy gọi nhau bằng tên
    environment: {ROLE: master}                        # entrypoint biết đây là master
    volumes: [..:/opt/work, master-data:/data]         # project gắn vào /opt/work; HDFS trong volume
    ports: ["9870:9870", "8080:8080", "18080:18080", "4040:4040"]   # web UI ra laptop
    depends_on: [worker1, worker2]                     # bật worker trước
  worker1: {image: …, hostname: worker1, volumes: [..:/opt/work, worker1-data:/data]}
  worker2: …
volumes: {master-data:, worker1-data:, worker2-data:}  # dữ liệu HDFS còn lại sau make down
```
- Podman tạo một mạng riêng cho 3 container; mỗi container gọi nhau bằng tên (`master`, `worker1`…).
- Thư mục project được **gắn** (không copy) vào `/opt/work`: sửa code trên laptop là container thấy ngay, kết quả ghi vào `results/dev/` trên laptop.

**`.dockerignore`**: build image chỉ gửi `cluster/` và `dev/` cho Podman, không gửi cả project (results, `.venv`…).

## 5. AWS: Terraform (`infra/terraform/`)

Terraform đọc các file `.tf`, so với trạng thái đã lưu (`terraform.tfstate`), rồi gọi API AWS để tạo/sửa/xoá cho khớp. Lệnh: `terraform init` (tải plugin), `terraform apply` (tạo, hỏi `yes`), `terraform destroy` (xoá), `terraform output` (đọc giá trị xuất ra).

**`versions.tf`**: dùng plugin `aws` (gọi API AWS) và `local` (ghi file trên laptop); vùng lấy từ biến `region`.

**`variables.tf`**

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `region` | `ap-southeast-2` (Sydney) | vùng duy nhất tài khoản Free plan được dùng |
| `my_ip_cidr` | (Makefile truyền vào) | IP laptop + `/32` (đúng một địa chỉ) |
| `instance_type` | `m7i-flex.large` | loại máy (2 vCPU, 8 GB; có trong Free plan) |
| `worker_count` | 7 | số worker (1 + 7 máy × 2 vCPU = 16 = quota) |
| `disk_gb` | 100 | dung lượng đĩa mỗi máy (dữ liệu ≈ 38 GB mỗi worker) |

**`main.tf`**, từng khối:

```hcl
locals { names = concat(["master"], [for i in range(1, var.worker_count + 1) : "worker${i}"]) }
```
Danh sách tên máy: `["master", "worker1", "worker2", "worker3"]`.

```hcl
data "aws_ami" "ubuntu" { most_recent = true  owners = ["099720109477"]  filter { name = "name"  values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"] } }
```
`data`: **tìm** (không tạo). Image Ubuntu 22.04 x86_64 mới nhất do Canonical (công ty làm Ubuntu, mã chủ sở hữu `099720109477`) phát hành.

```hcl
resource "aws_vpc" "main"           { cidr_block = "10.0.0.0/16" … }       # mạng riêng ảo, IP 10.0.x.x
resource "aws_subnet" "main"        { cidr_block = "10.0.1.0/24"  availability_zone = "${var.region}a"
                                      map_public_ip_on_launch = true … }   # mạng con trong 1 zone; máy có IP công khai
resource "aws_internet_gateway" "main" { vpc_id = … }                      # cổng ra Internet
resource "aws_route_table" "main"   { route { cidr_block = "0.0.0.0/0"  gateway_id = <gateway> } }   # mọi địa chỉ ngoài → qua cổng
resource "aws_route_table_association" "main" { … }                        # gắn bảng định tuyến vào mạng con
```
Năm khối này tạo một **mạng riêng** có lối ra Internet (cần vì tài khoản không có mạng mặc định). Mọi máy trong **cùng một availability zone** (`ap-southeast-2a`): mạng giữa các máy như nhau, không mất phí truyền dữ liệu giữa các zone.

```hcl
resource "aws_security_group" "cluster" {
  ingress { from_port = 22  to_port = 22  protocol = "tcp"  cidr_blocks = [var.my_ip_cidr] }   # SSH: chỉ từ IP của bạn
  ingress { from_port = 0   to_port = 0   protocol = "-1"   self = true }                     # mọi cổng: giữa các máy trong nhóm
  egress  { from_port = 0   to_port = 0   protocol = "-1"   cidr_blocks = ["0.0.0.0/0"] }      # đi ra: tự do (apt, tải Hadoop)
}
```
**Tường lửa** (giống *Security Group* trong lab AWS của môn). `protocol = "-1"`: mọi giao thức. HDFS, Spark, shuffle dùng nhiều cổng giữa các máy nên mở hết **trong nội bộ** cluster, còn từ ngoài chỉ có SSH từ IP của bạn.

```hcl
resource "aws_key_pair" "key" { key_name = "cloud-bigdata-project"  public_key = file(pathexpand("~/.ssh/cloud-bigdata-project.pub")) }
```
Đưa **khoá công khai** của laptop lên AWS; AWS cài nó vào máy mới để bạn SSH vào bằng khoá riêng tương ứng.

```hcl
resource "aws_instance" "node" {
  for_each               = toset(local.names)          # một máy cho mỗi tên
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.key.key_name
  subnet_id              = aws_subnet.main.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  root_block_device { volume_type = "gp3"  volume_size = var.disk_gb }   # đĩa gp3: tốc độ cố định
  tags = { Name = "cloud-bigdata-project-${each.key}" }                   # tên hiện trên console
}
```
`gp3`: loại đĩa có tốc độ cố định 3000 IOPS / 125 MB/s (loại `gp2` cũ có "credit" tốc độ cạn dần, làm số đo thay đổi).

```hcl
resource "local_file" "inventory" { filename = "…/ansible/inventory.ini"  content = <<-EOT … EOT }
```
Ghi file **inventory** cho Ansible: danh sách máy, IP công khai (để Ansible SSH vào), IP riêng (để ghi `/etc/hosts`). Ví dụ:
```ini
[masters]
master ansible_host=3.25.245.251 private_ip=10.0.1.84
[workers]
worker1 ansible_host=… private_ip=10.0.1.37
…
[all:vars]
ansible_user=ubuntu
ansible_ssh_private_key_file=~/.ssh/cloud-bigdata-project
```

```hcl
output "master_public_ip" { value = aws_instance.node["master"].public_ip }
```
Makefile đọc giá trị này (`terraform output -raw master_public_ip`) để SSH vào master.

## 6. AWS: Ansible (`infra/ansible/`)

Ansible SSH từ laptop vào các máy trong `inventory.ini` và thực hiện từng bước (*task*), theo từng nhóm bước (*play*). `ansible.cfg`: dùng `inventory.ini`, không hỏi xác nhận máy lạ, không in cảnh báo về Python.

**Play 1: chờ máy sẵn sàng**
```yaml
- hosts: all
  gather_facts: false                       # chưa thu thông tin máy (chưa SSH được)
  tasks:
    - ansible.builtin.wait_for_connection: {timeout: 300}   # thử SSH đến khi được, tối đa 5 phút
```
Máy EC2 vừa tạo cần khoảng một phút mới nhận SSH.

**Play 2: mọi máy** (`become: true` = chạy bằng root qua `sudo`)
```yaml
- hostname: {name: "{{ inventory_hostname }}"}      # đặt tên máy = master / worker1 …
- copy:                                              # /etc/hosts: tên → IP riêng của mọi máy
    dest: /etc/hosts
    content: |
      127.0.0.1 localhost
      {% for host in groups['all'] %}
      {{ hostvars[host].private_ip }} {{ host }}
      {% endfor %}
```
Nhờ `/etc/hosts`, `master`, `worker1`… dùng được như tên máy ở mọi nơi (cấu hình Hadoop, Spark, file `workers`). Giống TP Ansible (sửa `/etc/hosts`).

```yaml
- systemd: {name: "{{ item }}", state: stopped, enabled: false}
  loop: [unattended-upgrades.service, apt-daily.timer, apt-daily-upgrade.timer]
  failed_when: false
```
Tắt cập nhật tự động của Ubuntu: nó có thể chạy giữa lúc đo, chiếm CPU/đĩa/mạng.

```yaml
- copy: {src: "{{ playbook_dir }}/../../cluster/", dest: /opt/cluster/}   # đưa cluster/ lên máy
- command: bash /opt/cluster/install.sh ubuntu                            # CÙNG script với local
- copy:                                                                   # file workers: worker1, worker2, …
    dest: "{{ item }}"
    owner: ubuntu
    content: "{{ groups['workers'] | join('\n') }}\n"
  loop: [/opt/hadoop/etc/hadoop/workers, /opt/spark/conf/workers]
```
Giống TP Ansible: lệnh cài nằm trong một script shell, playbook đưa lên và chạy. File `workers` tạo từ danh sách máy của inventory (đổi `worker_count` là tự đúng).

**Play 3: SSH không mật khẩu từ master** (chạy bằng user `ubuntu`)
```yaml
- command: ssh-keygen -t ed25519 -N "" -f /home/ubuntu/.ssh/id_ed25519   # tạo khoá trên master (một lần)
  args: {creates: /home/ubuntu/.ssh/id_ed25519}
  when: inventory_hostname == 'master'
- slurp: {src: /home/ubuntu/.ssh/id_ed25519.pub}                          # đọc khoá công khai của master
  register: master_key
  when: inventory_hostname == 'master'
- authorized_key: {user: ubuntu, key: "{{ hostvars['master'].master_key.content | b64decode }}"}   # cho phép khoá đó vào mọi máy
- copy: {dest: /home/ubuntu/.ssh/config, content: "Host *\n  StrictHostKeyChecking accept-new\n"}  # master không hỏi khi SSH lần đầu
  when: inventory_hostname == 'master'
```
Tự động hoá đúng việc TP làm bằng tay với `ssh-keygen` + `ssh-copy-id`. Cần cho `start-dfs.sh`, `start-workers.sh` và `run_experiments.py` (SSH sang worker để đo).

**Play 4: trên master**
```yaml
- command: bash -l /opt/cluster/start-cluster.sh            # CÙNG script với local
- synchronize: {src: "…/{{ item }}", dest: /opt/work/}      # rsync java/ và bench/ lên master
  loop: [java, bench]
- command: bash -l java/build.sh                            # build jar
  args: {chdir: /opt/work}
```

## 7. `Makefile`: mọi lệnh trên laptop

```make
ENV ?= dev                                   # mặc định local; make … ENV=aws để chọn AWS
DC = podman compose -f dev/compose.yml
KEY = $(HOME)/.ssh/cloud-bigdata-project
MASTER_IP = $(shell terraform -chdir=infra/terraform output -raw master_public_ip 2>/dev/null)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER_IP)
TF = TF_VAR_my_ip_cidr=$$(curl -s https://checkip.amazonaws.com)/32 terraform -chdir=infra/terraform
```
- `MASTER_IP`: hỏi Terraform IP của master (rỗng nếu chưa có cluster).
- `TF`: lấy IP công khai hiện tại của laptop (dịch vụ `checkip.amazonaws.com` của AWS trả về IP của người gọi), truyền vào biến `my_ip_cidr` qua biến môi trường `TF_VAR_my_ip_cidr`.
- `$$`: trong Makefile, `$` phải viết `$$` để đến được shell.

**Với `ENV=aws`:**
```make
up: $(KEY)                                         # cần khoá SSH trước (tự tạo nếu chưa có)
	$(TF) init
	$(TF) apply
	cd infra/ansible && ansible-playbook cluster.yml
run: cluster-ip
	$(SSH) "tmux new -d -s exp 'bash -lc \"cd /opt/work && mkdir -p results/aws && bash bench/run_all.sh aws 2>&1 | tee -a results/aws/run.log\"'"
```
- `tmux new -d -s exp '…'`: mở một phiên tmux tên `exp` **chạy nền** (`-d`) trên master. Ngắt SSH, tắt laptop: phiên vẫn chạy.
- `2>&1 | tee -a results/aws/run.log`: mọi output (cả lỗi) vừa hiện vừa ghi thêm vào `run.log`.

```make
status: cluster-ip
	@$(SSH) "tmux ls …; grep -E '^(built|written|dataset|warm-up|\[[0-9]+/[0-9]+\])| done$$|Exception' …/run.log | tail -n 8; echo runs ok: \$$(grep -c ',ok,' …/runs.csv …)"
```
Chỉ lọc các dòng tiến độ trong log (bỏ log của Spark) và đếm số lần đo `ok`.

```make
fetch: cluster-ip
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER_IP):/opt/work/results/aws/ results/aws/
shell: cluster-ip
	$(SSH) -L 8080:master:8080 -L 18080:master:18080 -L 9870:master:9870
down:
	$(TF) destroy
cluster-ip:
	@test -n "$(MASTER_IP)" || { echo "no EC2 cluster: run make up ENV=aws first"; exit 1; }
$(KEY):
	ssh-keygen -t ed25519 -N "" -C cloud-bigdata-project -f $(KEY)
```
- `rsync -az`: chép đệ quy, nén khi truyền, chỉ chép phần thay đổi.
- `-L 8080:master:8080`: **SSH tunnel**: http://localhost:8080 trên laptop đi qua SSH tới cổng 8080 của master. Web UI không phải mở ra Internet.
- `cluster-ip`: báo lỗi rõ ràng nếu chưa có cluster.
- `$(KEY):` là một *target là file*: chỉ chạy khi file khoá chưa tồn tại.

**Với `ENV=dev`:** `up` = `podman compose up -d --build`; `run` = `podman compose exec master bash -l bench/run_all.sh dev`; `shell` = vào container master; `down` = tắt container.

**Chung:**
```make
analysis: .venv/bin/python
	.venv/bin/python analysis/report.py results/$(ENV)
.venv/bin/python:
	/usr/bin/python3 -m venv .venv
	.venv/bin/pip install -q -r analysis/requirements.txt
```
Lần đầu tạo môi trường Python riêng (`.venv`) với pandas, matplotlib, PyYAML (phiên bản ghi trong `analysis/requirements.txt`).
