variable "project_id" {
  description = "GCP project that hosts the platform."
  type        = string
  default     = "aide-playground"
}

variable "region" {
  description = "Region for the state bucket. Same region as the platform to keep everything co-located."
  type        = string
  default     = "northamerica-northeast1"
}
