output "cluster_name" {
  value = google_container_cluster.primary.name
}

output "cluster_location" {
  value = google_container_cluster.primary.location
}

output "cluster_endpoint" {
  description = "API server address."
  value       = google_container_cluster.primary.endpoint
  sensitive   = true
}

output "workload_identity_pool" {
  description = "The cluster's Workload Identity pool, i.e. \"<project>.svc.id.goog\"."
  value       = "${var.project_id}.svc.id.goog"
}

output "get_credentials_command" {
  description = "Ready-to-run kubectl context setup."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.primary.name} --region ${var.region} --project ${var.project_id}"
}
