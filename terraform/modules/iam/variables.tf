variable "project_id" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "lakehouse_bucket_name" {
  type = string
}

variable "workload_identity_bindings" {
  description = "Kubernetes ServiceAccounts as \"<namespace>/<ksa-name>\"."
  type        = list(string)
}
