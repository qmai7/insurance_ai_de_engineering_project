##
# Lakehouse bucket — the GCS replacement for Part 1's MinIO.
#
# One bucket, prefix-separated by layer, rather than three buckets. Bronze,
# Silver, and Gold share identical access rules and lifecycle here, so separate
# buckets would add IAM surface and naming ceremony without buying isolation.
#
#   bronze/   raw generated source data (Parquet offline, JSONL streaming)
#   silver/   cleaned, quality-gated Delta tables
#   gold/     Gold exports out of ClickHouse
#   feast/    Feast offline store (plain Parquet)
#   delta/    versioned Gold->Delta training snapshots
#   mlflow/   MLflow run artifacts and model files
##

resource "google_storage_bucket" "lakehouse" {
  name     = "${var.project_id}-${var.bucket_suffix}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true

  # Every session starts from a regenerated dataset, so destroy must not stall on
  # a bucket full of objects. Without this, `terraform destroy` fails and the
  # cluster it was supposed to remove keeps billing.
  force_destroy = true

  # Object versioning stays OFF on purpose. Delta Lake's transaction log already
  # provides the data versioning the project needs, so GCS
  # versioning would store redundant copies of large Parquet files and bill for
  # every one.
  versioning {
    enabled = false
  }

  # New buckets default to a 7-day soft-delete window, and soft-deleted objects
  # are still billed as storage. For data that is regenerable from a seeded
  # generator, that is pure waste — a 0s retention disables it.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  # Backstop against a forgotten bucket quietly accruing storage charges after a
  # session ends without a destroy. 30 days is far longer than any single
  # session, so it never touches data in active use.
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels
}
