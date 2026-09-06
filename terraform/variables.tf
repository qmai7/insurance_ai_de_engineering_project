variable "project_id" {
  description = "GCP project that hosts the platform."
  type        = string
}

variable "region" {
  description = "Region for the regional Autopilot cluster and the lakehouse bucket."
  type        = string
  default     = "northamerica-northeast1"
}

variable "name_prefix" {
  description = "Prefix applied to every named resource, so unrelated things in the project are never mistaken for ours."
  type        = string
  default     = "insurance"
}

variable "github_repository" {
  description = "\"<owner>/<repo>\" — the only GitHub repo trusted by the Workload Identity Federation pool for CI image pushes (§8)."
  type        = string
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

variable "subnet_cidr" {
  description = "Primary range for the node subnet."
  type        = string
  default     = "10.10.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary range for Pod IPs. Sized generously — Autopilot allocates aggressively and the platform runs a lot of pods."
  type        = string
  default     = "10.20.0.0/14"
}

variable "services_cidr" {
  description = "Secondary range for ClusterIP Services."
  type        = string
  default     = "10.30.0.0/20"
}

variable "enable_private_nodes" {
  description = <<-EOT
    Whether cluster nodes get public IPs.

    Default is false (public nodes) purely for cost: private nodes cannot reach
    GHCR / PyPI / Maven without a Cloud NAT gateway, and NAT bills hourly plus
    per-GB. This platform pulls a LOT of images (Airflow, Spark, Flink, Kafka,
    ClickHouse, DataHub), so NAT data-processing charges are not trivial against
    a fixed credit.

    Flipping this to true automatically provisions the Cloud Router + NAT needed
    to keep egress working, so the flag is safe either way.
  EOT
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

variable "kubernetes_release_channel" {
  description = "GKE release channel. Autopilot requires one; REGULAR balances feature availability against surprise upgrades."
  type        = string
  default     = "REGULAR"

  validation {
    condition     = contains(["RAPID", "REGULAR", "STABLE"], var.kubernetes_release_channel)
    error_message = "Release channel must be RAPID, REGULAR, or STABLE."
  }
}

variable "master_authorized_networks" {
  description = <<-EOT
    CIDRs allowed to reach the Kubernetes API server. Empty list = open to any
    source (still authenticated, but reachable). Set this to your own egress IP
    for a tighter posture; left open by default so a changing home/office IP
    never locks you out mid-session.
  EOT
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = []
}

# ---------------------------------------------------------------------------
# Storage / IAM
# ---------------------------------------------------------------------------

variable "lakehouse_bucket_suffix" {
  description = "Suffix for the lakehouse bucket name; full name is \"<project_id>-<suffix>\" since GCS names are globally unique."
  type        = string
  default     = "lakehouse"
}

variable "workload_identity_bindings" {
  description = <<-EOT
    Kubernetes ServiceAccounts allowed to impersonate the data-platform GCP
    service account, as "<namespace>/<ksa-name>" strings.

    This is the list to extend as later build steps land — adding a workload
    means adding one entry here, not restructuring the IAM module.
  EOT
  type        = list(string)
  default = [
    "data-ns/airflow",
    "data-ns/spark",
  ]
}
