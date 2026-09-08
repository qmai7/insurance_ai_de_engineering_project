variable "project_id" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "github_repository" {
  description = "\"<owner>/<repo>\" — the only repository allowed to mint tokens against this pool."
  type        = string
}

variable "registry_location" {
  type = string
}

variable "registry_repository_id" {
  type = string
}
