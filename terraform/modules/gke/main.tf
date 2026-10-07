##
# Regional GKE Autopilot cluster.
#
# Autopilot suits this workload's shape: sessions are short and bursty, so
# per-pod billing beats paying for idle nodes. The tradeoffs it imposes — no
# node_config, no custom node pools, enforced resource requests, Workload
# Identity always on — are all acceptable or actively wanted here.
##

resource "google_container_cluster" "primary" {
  name     = "${var.name_prefix}-gke"
  project  = var.project_id
  location = var.region # regional: control plane replicated across zones

  enable_autopilot = true

  # Autopilot requires a release channel; it cannot run on a pinned version.
  release_channel {
    channel = var.release_channel
  }

  network    = var.network_id
  subnetwork = var.subnet_id

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pods_range_name
    services_secondary_range_name = var.services_range_name
  }

  private_cluster_config {
    enable_private_nodes = var.enable_private_nodes

    # The API server stays publicly reachable (still authenticated) so kubectl
    # works from anywhere without a bastion or VPN. A private endpoint would
    # mean no cluster access from a laptop, which makes per-session work painful.
    enable_private_endpoint = false
  }

  dynamic "master_authorized_networks_config" {
    for_each = length(var.master_authorized_networks) > 0 ? [1] : []
    content {
      dynamic "cidr_blocks" {
        for_each = var.master_authorized_networks
        content {
          cidr_block   = cidr_blocks.value.cidr_block
          display_name = cidr_blocks.value.display_name
        }
      }
    }
  }

  # Cost control, and a deliberate architectural choice.
  #
  # Cloud Logging bills per GB ingested. Kafka, Flink, Spark, and Airflow are
  # chatty, and the platform already routes application logs to Loki and traces
  # to Tempo by design. Shipping workload logs to Cloud Logging as well would be
  # paying twice for the same data. System-component logs stay on — they are how
  # you debug the cluster itself when nothing else is up yet.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }

  # Metrics are the same argument, but Autopilot does not allow acting on it:
  # Google Managed Prometheus is mandatory on Autopilot 1.25+ and the API
  # rejects `enabled = false` outright. So GMP runs alongside the self-hosted
  # Prometheus that the observability work uses.
  #
  # Scope is still held to SYSTEM_COMPONENTS, which is the part that is
  # controllable — GMP bills per sample ingested, and system-component metrics
  # are a small, fixed volume compared to scraping every workload pod.
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]

    managed_prometheus {
      enabled = true
    }
  }

  # This cluster is torn down at the end of every work session, which is the
  # entire cost strategy. Deletion protection defaults to true and would make
  # `terraform destroy` fail — leaving a cluster billing overnight.
  deletion_protection = false

  resource_labels = var.labels

  # Autopilot regional cluster creation is slow (~10-15 min) and deletion is not
  # much faster. Generous timeouts avoid a spurious failure mid-apply that leaves
  # state disagreeing with reality.
  timeouts {
    create = "40m"
    update = "40m"
    delete = "40m"
  }
}
