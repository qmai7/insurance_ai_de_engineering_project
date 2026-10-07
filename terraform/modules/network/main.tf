##
# Dedicated VPC for the platform.
#
# A purpose-built VPC rather than the project's default network: secondary
# ranges are declared explicitly (so Pod/Service CIDRs are reviewable rather
# than auto-assigned), and `terraform destroy` removes the whole network instead
# of leaving mutated shared infrastructure behind.
##

resource "google_compute_network" "vpc" {
  name                    = "${var.name_prefix}-vpc"
  project                 = var.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
  description             = "VPC for the insurance fraud detection platform (ephemeral)."
}

resource "google_compute_subnetwork" "nodes" {
  name    = "${var.name_prefix}-subnet-${var.region}"
  project = var.project_id
  region  = var.region
  network = google_compute_network.vpc.id

  ip_cidr_range = var.subnet_cidr

  # VPC-native cluster ranges. Autopilot is always VPC-native, and naming the
  # ranges here means the cluster references them by name instead of GKE
  # silently carving new ones out of the subnet.
  secondary_ip_range {
    range_name    = "${var.name_prefix}-pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "${var.name_prefix}-services"
    ip_cidr_range = var.services_cidr
  }

  # Required for private nodes to reach Google APIs (GCS, Artifact Registry)
  # without a public IP. Harmless and free when nodes are public, so it is
  # always on rather than coupled to the flag.
  private_ip_google_access = true
}

# ---------------------------------------------------------------------------
# Egress for private nodes
#
# Only created when private nodes are requested. Without NAT, private nodes
# cannot pull from GHCR / PyPI / Maven and every pod lands in ImagePullBackOff —
# so the flag provisions its own prerequisite rather than half-working.
# ---------------------------------------------------------------------------

resource "google_compute_router" "router" {
  count = var.enable_private_nodes ? 1 : 0

  name    = "${var.name_prefix}-router"
  project = var.project_id
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  count = var.enable_private_nodes ? 1 : 0

  name    = "${var.name_prefix}-nat"
  project = var.project_id
  region  = var.region
  router  = google_compute_router.router[0].name

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = false # NAT logs are pure cost here; Loki covers application logging.
    filter = "ERRORS_ONLY"
  }
}
