##
# Artifact Registry — container images for every workload on the platform.
#
# Chosen over GHCR (CLAUDE.md §8's original pick) for two concrete reasons:
# pushing needs no personal access token because Docker authenticates through
# gcloud, and GKE pulls need no imagePullSecret because the node service account
# is granted read access directly. Both of those are credentials that would
# otherwise have to be created, stored, and rotated by hand.
##

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = "${var.name_prefix}-images"
  format        = "DOCKER"
  description   = "Container images for the insurance fraud detection platform."

  # Artifact Registry bills for stored bytes, and the Airflow image is large and
  # rebuilt often during development. Untagged versions are pure waste — they are
  # what a retagged push leaves behind — so they are cleaned up aggressively,
  # while tagged releases are kept long enough to roll back to.
  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"
    condition {
      tag_state  = "UNTAGGED"
      older_than = "86400s" # 1 day
    }
  }

  cleanup_policies {
    id     = "keep-recent-tagged"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }

  labels = var.labels
}

# GKE nodes pull images as the Compute Engine default service account. Granting
# the role on the repository rather than the project keeps the node identity's
# reach limited to exactly this one repository.
resource "google_artifact_registry_repository_iam_member" "node_puller" {
  project    = var.project_id
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
}
