# 1 master + N workers on EC2, in the default VPC of the account.
locals {
  names = concat(["master"], [for i in range(1, var.worker_count + 1) : "worker${i}"])
}

# Ubuntu 22.04 x86_64 (x86_64 because Hadoop's native library, needed by zstd, is built for it)
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

data "aws_vpc" "default" {
  default = true
}

# SSH only from my IP; all traffic between the nodes of the cluster (HDFS, Spark, shuffle).
resource "aws_security_group" "cluster" {
  name   = "compression-study"
  vpc_id = data.aws_vpc.default.id

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
  key_name   = "compression-study"
  public_key = file(pathexpand("~/.ssh/compression-study.pub"))
}

resource "aws_instance" "node" {
  for_each               = toset(local.names)
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.key.key_name
  vpc_security_group_ids = [aws_security_group.cluster.id]

  root_block_device {
    volume_type = "gp3" # constant 3000 IOPS / 125 MB/s, no burst credits
    volume_size = var.disk_gb
  }

  tags = { Name = "compression-study-${each.key}" }
}

# Ansible inventory: public IP to connect, private IP for /etc/hosts.
resource "local_file" "inventory" {
  filename = "${path.module}/../ansible/inventory.ini"
  content  = <<-EOT
    [masters]
    master ansible_host=${aws_instance.node["master"].public_ip} private_ip=${aws_instance.node["master"].private_ip}

    [workers]
    %{for name in slice(local.names, 1, length(local.names))~}
    ${name} ansible_host=${aws_instance.node[name].public_ip} private_ip=${aws_instance.node[name].private_ip}
    %{endfor~}

    [all:vars]
    ansible_user=ubuntu
    ansible_ssh_private_key_file=~/.ssh/compression-study
  EOT
}

output "master_public_ip" {
  value = aws_instance.node["master"].public_ip
}
