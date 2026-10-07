##
# Workload Identity Federation for GitHub Actions — §8's "no PAT to push"
# credential, the CI-side counterpart to modules/iam's in-cluster Workload
# Identity.
#
# The chain: GitHub mints a short-lived OIDC token for the workflow run: the
# pool + provider here tell Google to trust that token if (and only if) it
# came from this exact repo; the attribute condition enforces that so a
# workflow in some other repo in the same GitHub org can't mint a matching
# token. `google_service_account_iam_member` then lets *that* narrowed
# identity impersonate a dedicated GCP service account, which is the only
# principal actually granted push access to Artifact Registry. No JSON key
# is created, downloaded, or stored in a GitHub secret — same reasoning as
# modules/iam, applied to the CI side of the platform instead of the runtime
# side.
##

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.project_id
  workload_identity_pool_id = "${var.name_prefix}-github"
  display_name              = "GitHub Actions"
  description               = "Federates GitHub Actions OIDC tokens for CI image pushes."
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub Actions OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  # Narrows *at the token-verification level*, not just at the IAM-binding
  # level — a token whose `repository` claim isn't this one is rejected before
  # any IAM policy is even consulted.
  attribute_condition = "assertion.repository == \"${var.github_repository}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "github_actions_ci" {
  account_id   = "${var.name_prefix}-github-ci"
  project      = var.project_id
  display_name = "GitHub Actions CI"
  description  = "Impersonated by GitHub Actions via Workload Identity Federation to push images."
}

# roles/iam.workloadIdentityUser scoped to the one pool + repository condition
# above, not to the whole project's Workload Identity Pools.
resource "google_service_account_iam_member" "github_actions_wif" {
  service_account_id = google_service_account.github_actions_ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}

# Repository-scoped, not project-scoped: this identity can push images to the
# one Artifact Registry repo the platform uses, and nothing else.
resource "google_artifact_registry_repository_iam_member" "github_ci_writer" {
  project    = var.project_id
  location   = var.registry_location
  repository = var.registry_repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.github_actions_ci.email}"
}
