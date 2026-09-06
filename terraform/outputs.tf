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

output "registry_url" {
  description = "Base URL for image tags."
  value       = module.registry.repository_url
}

output "docker_auth_command" {
  description = "Run once per machine before pushing images."
  value       = module.registry.docker_auth_command
}

output "github_actions_workload_identity_provider" {
  description = "Set as the GH_WORKLOAD_IDENTITY_PROVIDER repo variable / google-github-actions/auth's workload_identity_provider input."
  value       = module.ci.workload_identity_provider
}

output "github_actions_service_account" {
  description = "Set as the GH_CI_SERVICE_ACCOUNT repo variable / google-github-actions/auth's service_account input."
  value       = module.ci.service_account_email
}
