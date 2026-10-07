variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "subnet_cidr" {
  type = string
}

variable "pods_cidr" {
  type = string
}

variable "services_cidr" {
  type = string
}

variable "enable_private_nodes" {
  description = "When true, provisions Cloud Router + NAT so private nodes retain outbound internet access."
  type        = bool
}
