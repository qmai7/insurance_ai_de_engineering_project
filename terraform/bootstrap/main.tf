##
# Bootstrap — the ONE thing that must outlive `terraform destroy`.
#
# Creates the GCS bucket that holds remote state for the main platform config.
# Run this once, ever. It uses LOCAL state (terraform.tfstate next to this file)
# because there is no remote backend to store it in yet — the classic
# chicken-and-egg. That local state file is small, boring, and gitignored; if you
# lose it the only cost is re-importing or hand-deleting one bucket.
#
#   cd terraform/bootstrap && terraform init && terraform apply
#
# Do NOT run `terraform destroy` here between work sessions. Destroying this
# bucket throws away the platform's state, which orphans billable resources —
# the exact failure mode the ephemeral-cluster plan cannot tolerate.
##

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "tfstate" {
  name     = "${var.project_id}-tfstate"
  project  = var.project_id
  location = var.region

  # State files must be versioned. An interrupted apply can leave a corrupt or
  # truncated state object; versioning is what lets you roll back to the last
  # good generation instead of rebuilding the platform by hand.
  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true

  # Guardrail, not paranoia: `force_destroy` stays false so a stray
  # `terraform destroy` in this directory fails loudly on a non-empty bucket
  # rather than quietly deleting the state that tracks everything else.
  force_destroy = false

  # Keep only recent state generations so this bucket stays effectively free.
  lifecycle_rule {
    condition {
      num_newer_versions = 20
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project   = "insurance-fraud-platform"
    component = "terraform-state"
    lifecycle = "persistent"
  }
}