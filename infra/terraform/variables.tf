variable "region" {
  default = "eu-west-3" # Paris
}

variable "my_ip_cidr" {
  description = "My public IP + /32 (curl -s https://checkip.amazonaws.com), the only address allowed to SSH"
}

variable "instance_type" {
  default = "m5.xlarge" # 4 vCPU, 16 GB; not a burstable t3 (CPU credits would distort the measurements)
}

variable "worker_count" {
  default = 3
}

variable "disk_gb" {
  default = 200 # SF 1+5+10+20 x 5 codecs ~ 88 GB per worker with 3 HDFS replicas, + shuffle files
}
