variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "bucket_suffix" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}
