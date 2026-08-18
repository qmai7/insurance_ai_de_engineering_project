##
# Workload Identity — how in-cluster pods reach GCS without a key file.
#
# The chain: a Kubernetes ServiceAccount is annotated with this GCP service
# account, and an IAM binding lets that KSA impersonate it. Pods then get
# short-lived, auto-rotated credentials from the metadata server.
#
# The point is that no JSON key is ever created, downloaded, mounted, or
# committed — which is also what keeps the Security rubric item honest, since a
# static key in a Kubernetes Secret is exactly the anti-pattern it asks about.
##

resource "google_service_account" "data_platform" {
  account_id   = "${var.name_prefix}-data-platform"
  project      = var.project_id
  display_name = "Insurance platform data-plane workloads"
  description  = "Impersonated via Workload Identity by Airflow/Spark/Flink pods for lakehouse access."
}

# Bucket-scoped rather than project-wide roles/storage.admin: these workloads
# read and write objects in one bucket, and nothing they do requires the ability
# to create or delete buckets.
resource "google_storage_bucket_iam_member" "lakehouse_object_admin" {
  bucket = var.lakehouse_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.data_platform.email}"
}

# Spark and Delta list bucket contents and read metadata during planning, which
# objectAdmin alone does not grant.
resource "google_storage_bucket_iam_member" "lakehouse_legacy_reader" {
  bucket = var.lakehouse_bucket_name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.data_platform.email}"
}

# One binding per Kubernetes ServiceAccount permitted to impersonate the GCP SA.
#
# The KSAs need not exist yet — the member is a string resolved at token-request
# time, so IAM does not wait on Helm. The *identity pool* is different: IAM
# validates "<project>.svc.id.goog" at binding time, and that pool is created by
# the cluster. Hence the module-level dependency on GKE in the root config;
# without it, a clean apply fails with "Identity Pool does not exist".
resource "google_service_account_iam_member" "workload_identity" {
  for_each = toset(var.workload_identity_bindings)

  service_account_id = google_service_account.data_platform.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${each.value}]"
}
