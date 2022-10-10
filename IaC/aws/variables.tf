locals {
  cluster_name = "eks-xpe-mba2022-${random_string.suffix.result}"
}

resource "random_string" "suffix" {
  length  = 2
  special = false
}
variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}
variable "subnet_id_1" {
  type    = string
  default = "subnet-0daab00e27637c362"
}

variable "subnet_id_2" {
  type    = string
  default = "subnet-083f2a55a8fa68dd4"
}