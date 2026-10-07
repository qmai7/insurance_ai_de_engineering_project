output "bucket_name" {
  value = google_storage_bucket.lakehouse.name
}

output "bucket_url" {
  description = "gs:// URI, the form Spark/Flink/Feast configs consume."
  value       = "gs://${google_storage_bucket.lakehouse.name}"
}
