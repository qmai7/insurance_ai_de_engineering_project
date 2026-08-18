output "project_id" {
  value = var.project_id
}

output "cluster_name" {
  value = module.gke.cluster_name
}

output "cluster_location" {
  value = module.gke.cluster_location
}

output "get_credentials_command" {
  description = "Run this to point kubectl at the cluster."
  value       = module.gke.get_credentials_command
}

output "lakehouse_bucket" {
  value = module.storage.bucket_name
}

output "lakehouse_url" {
  value = module.storage.bucket_url
}

output "data_platform_service_account" {
  description = "GCP service account that in-cluster workloads impersonate via Workload Identity."
  value       = module.iam.service_account_email
}

output "ksa_annotation" {
  description = "Annotation to add to each Kubernetes ServiceAccount listed in workload_identity_bindings."
  value       = module.iam.ksa_annotation
}

output "network_name" {
  value = module.network.network_name
}
