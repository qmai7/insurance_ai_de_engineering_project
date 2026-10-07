output "state_bucket" {
  description = "Bucket name to reference in the platform config's `backend \"gcs\"` block."
  value       = google_storage_bucket.tfstate.name
}
