variable "region" {
  default = "ap-southeast-2"
}

variable "my_ip_cidr" {
}

variable "instance_type" {
  default = "m7i-flex.large"
}

variable "worker_count" {
  default = 7
}

variable "disk_gb" {
  default = 100
}
