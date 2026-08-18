##
# Insurance Fraud Detection Platform — Step 1 infrastructure.
#
# Scope is deliberately narrow: GKE Autopilot cluster, lakehouse bucket, and the
# IAM needed to let in-cluster workloads read/write that bucket. Nothing else.
##

locals {
  common_labels = {
    project    = "insurance-fraud-platform"
    managed_by = "terraform"
    lifecycle  = "ephemeral"
  }
}

# ---------------------------------------------------------------------------
# Project APIs
#
# disable_on_destroy = false everywhere: turning an API off is project-wide and
# would break anything else living in this project. Destroy should remove our
# resources, not reconfigure the project.
# ---------------------------------------------------------------------------

resource "google_project_service" "required" {
  for_each = toset([
    "compute.googleapis.com",
    "container.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "artifactregistry.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  disable_on_destroy         = false
  disable_dependent_services = false
}

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

module "network" {
  source = "./modules/network"

  project_id           = var.project_id
  region               = var.region
  name_prefix          = var.name_prefix
  subnet_cidr          = var.subnet_cidr
  pods_cidr            = var.pods_cidr
  services_cidr        = var.services_cidr
  enable_private_nodes = var.enable_private_nodes

  depends_on = [google_project_service.required]
}

module "storage" {
  source = "./modules/storage"

  project_id    = var.project_id
  region        = var.region
  bucket_suffix = var.lakehouse_bucket_suffix
  labels        = local.common_labels

  depends_on = [google_project_service.required]
}

module "registry" {
  source = "./modules/registry"

  project_id  = var.project_id
  region      = var.region
  name_prefix = var.name_prefix
  labels      = local.common_labels

  depends_on = [google_project_service.required]
}

module "gke" {
  source = "./modules/gke"

  project_id                 = var.project_id
  region                     = var.region
  name_prefix                = var.name_prefix
  network_id                 = module.network.network_id
  subnet_id                  = module.network.subnet_id
  pods_range_name            = module.network.pods_range_name
  services_range_name        = module.network.services_range_name
  enable_private_nodes       = var.enable_private_nodes
  master_authorized_networks = var.master_authorized_networks
  release_channel            = var.kubernetes_release_channel
  labels                     = local.common_labels

  depends_on = [google_project_service.required]
}

module "iam" {
  source = "./modules/iam"

  project_id                 = var.project_id
  name_prefix                = var.name_prefix
  lakehouse_bucket_name      = module.storage.bucket_name
  workload_identity_bindings = var.workload_identity_bindings

  # The cluster, not just the APIs: IAM validates the Workload Identity pool
  # ("<project>.svc.id.goog") when the binding is created, and that pool does not
  # exist until a Workload-Identity-enabled cluster does.
  depends_on = [google_project_service.required, module.gke]
}
