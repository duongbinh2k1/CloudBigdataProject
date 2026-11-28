variable "region" {
  default = "ap-southeast-2"
}

variable "my_ip_cidr" {
  description = "My public IP + /32, the only address allowed to SSH (set by make up)"
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
