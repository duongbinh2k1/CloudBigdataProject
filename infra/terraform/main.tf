# Ubuntu 22.04 x86_64 (x86_64 because Hadoop's native library, needed by zstd, is built for it)
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# network: one public subnet in one availability zone
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "cloud-bigdata-project" }
}

resource "aws_subnet" "main" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.region}a" # all nodes in the same zone (same network, no cross-zone cost)
  map_public_ip_on_launch = true
  tags                    = { Name = "cloud-bigdata-project" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "main" {
  subnet_id      = aws_subnet.main.id
  route_table_id = aws_route_table.main.id
}

# SSH only from my IP; all traffic between the nodes of the cluster (HDFS, Spark, shuffle).
resource "aws_security_group" "cluster" {
  name   = "cloud-bigdata-project"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "key" {
  key_name   = "cloud-bigdata-project"
  public_key = file(pathexpand("~/.ssh/cloud-bigdata-project.pub"))
}

# the master: HDFS NameNode, Spark Master, History Server, driver of the jobs
resource "aws_instance" "master" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.key.key_name
  subnet_id              = aws_subnet.main.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  root_block_device {
    volume_type = "gp3" # constant 3000 IOPS / 125 MB/s
    volume_size = var.disk_gb
  }
  tags = { Name = "cloud-bigdata-project-master" }
}

# the workers: HDFS DataNode + Spark Worker
resource "aws_instance" "worker" {
  count                  = var.worker_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.key.key_name
  subnet_id              = aws_subnet.main.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  root_block_device {
    volume_type = "gp3"
    volume_size = var.disk_gb
  }
  tags = { Name = "cloud-bigdata-project-worker${count.index + 1}" }
}

# Ansible inventory: public IP to connect, private IP for /etc/hosts
resource "local_file" "inventory" {
  filename = "${path.module}/../ansible/inventory.ini"
  content  = <<-EOT
    [masters]
    master ansible_host=${aws_instance.master.public_ip} private_ip=${aws_instance.master.private_ip}

    [workers]
    %{for i, w in aws_instance.worker~}
    worker${i + 1} ansible_host=${w.public_ip} private_ip=${w.private_ip}
    %{endfor~}

    [all:vars]
    ansible_user=ubuntu
    ansible_ssh_private_key_file=~/.ssh/cloud-bigdata-project
  EOT
}

# list of the workers, copied by Ansible to Hadoop and Spark
resource "local_file" "workers" {
  filename = "${path.module}/../ansible/workers"
  content  = <<-EOT
    %{for i, w in aws_instance.worker~}
    worker${i + 1}
    %{endfor~}
  EOT
}

output "master_public_ip" {
  value = aws_instance.master.public_ip
}
