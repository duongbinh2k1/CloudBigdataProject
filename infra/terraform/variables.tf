variable "region" {
  # Sydney: the only region allowed on this account's Free plan (Singapore, ap-southeast-1,
  # is closer to Vietnam: use it if the account allows it, e.g. after upgrading the plan)
  default = "ap-southeast-2"
}

variable "my_ip_cidr" {
  description = "My public IP + /32, the only address allowed to SSH (set by make up ENV=aws)"
}

variable "instance_type" {
  # TEMPORARY: m7i-flex.large (2 vCPU, 8 GB) is allowed on the AWS Free plan, to test the cluster.
  # For the measurements of the report: m5.xlarge (4 vCPU, 16 GB, constant CPU, not "flex" or burstable t3).
  default = "m7i-flex.large"
}

variable "worker_count" {
  default = 3
}

variable "disk_gb" {
  default = 200 # SF 1+5+10+20 x 5 codecs ~ 88 GB per worker with 3 HDFS replicas, + shuffle files
}
