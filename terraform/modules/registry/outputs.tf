output "repository_url" {
  description = "Base URL for image tags, e.g. \"<region>-docker.pkg.dev/<project>/<repo>\"."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "docker_auth_command" {
  description = "Run once per machine so docker push can authenticate."
  value       = "gcloud auth configure-docker ${var.region}-docker.pkg.dev"
}
