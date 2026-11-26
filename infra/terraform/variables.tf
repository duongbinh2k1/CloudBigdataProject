variable "region" {
  # the only region allowed on this Free plan account (Singapore would be closer)
  default = "ap-southeast-2"
}

variable "my_ip_cidr" {
  description = "My public IP + /32, the only address allowed to SSH (set by make up)"
}

variable "instance_type" {
  # 2 vCPU, 8 GB, allowed on the AWS Free plan (m5 is not). CPU not limited by credits like t3, but disk
  # (39 MB/s) and network (0.39 Gbps) fall back to their baseline once the burst credits are used
  default = "m7i-flex.large"
}

variable "worker_count" {
  default = 7 # 1 master + 7 workers = 16 vCPU, the quota of the account
}

variable "disk_gb" {
  default = 100 # SF 1+5+10+20 x 3 copies (none, lz4, zstd) ~ 62 GB, x 3 HDFS replicas / 7 workers ~ 27 GB per worker, + output and shuffle
}
