output "service_account_email" {
  description = "Value for the KSA's iam.gke.io/gcp-service-account annotation."
  value       = google_service_account.data_platform.email
}

output "ksa_annotation" {
  description = "The exact annotation line to add to each Kubernetes ServiceAccount."
  value       = "iam.gke.io/gcp-service-account: ${google_service_account.data_platform.email}"
}
