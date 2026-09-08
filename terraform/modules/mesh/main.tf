##
# Managed Cloud Service Mesh.
#
# Google runs the mesh control plane; the cluster only gets the injected Envoy
# sidecars and the CRDs (DestinationRule, VirtualService, PeerAuthentication)
# that configure them. That is the whole reason it is three small resources here
# rather than an istioctl install or a Helm chart in charts/ — there is no
# istiod Deployment in this repo to version, resource-request or upgrade.
#
# Three resources, in a required order:
#
#   1. the APIs
#   2. a fleet membership — registering the cluster in the project's fleet, which
#      is the object mesh features attach to
#   3. the `servicemesh` fleet feature, set to MANAGEMENT_AUTOMATIC for that
#      membership
#
# All of it is inside Terraform state, so `terraform destroy` removes the
# membership with everything else. A mesh stood up by hand with `gcloud
# container fleet mesh enable` would survive the destroy, keep the fleet
# registered, and be exactly the kind of orphan §11 exists to prevent.
##

resource "google_project_service" "mesh" {
  for_each = var.enabled ? toset([
    # The fleet itself. Memberships live here.
    "gkehub.googleapis.com",
    # The managed control plane.
    "meshconfig.googleapis.com",
    # Issues the workload certificates that make mTLS work. Without it the
    # sidecars come up but every mTLS handshake fails with no obvious cause.
    "meshca.googleapis.com",
    # Mesh telemetry (§12). Enabled with the rest because turning it on later
    # requires a control-plane revision change, not just a flag.
    "meshtelemetry.googleapis.com",
    "monitoring.googleapis.com",
    "trafficdirector.googleapis.com",
  ]) : toset([])

  project = var.project_id
  service = each.value

  # Same rule as the other APIs in main.tf: destroy removes our resources, it
  # does not reconfigure the project.
  disable_on_destroy         = false
  disable_dependent_services = false
}

locals {
  # The membership the feature attaches to: an existing one when told about it,
  # otherwise the one created below.
  membership_id = var.existing_membership_id != null ? var.existing_membership_id : (
    var.enabled ? google_gke_hub_membership.cluster[0].membership_id : null
  )
}

resource "google_gke_hub_membership" "cluster" {
  # Not created when the cluster is already a fleet member. A second membership
  # for the same cluster is not a no-op — see existing_membership_id.
  count = var.enabled && var.existing_membership_id == null ? 1 : 0

  project = var.project_id
  # Named after the cluster, matching the convention `gcloud container fleet
  # memberships register` uses by default, so an imported membership and a
  # created one have the same name.
  membership_id = var.cluster_name

  endpoint {
    gke_cluster {
      resource_link = "//container.googleapis.com/projects/${var.project_id}/locations/${var.cluster_location}/clusters/${var.cluster_name}"
    }
  }

  depends_on = [google_project_service.mesh]
}

resource "google_gke_hub_feature" "mesh" {
  count = var.enabled ? 1 : 0

  project = var.project_id
  # The feature's name is fixed by the API — this is not a label we choose.
  name = "servicemesh"
  # Fleet features are global objects even when the cluster is regional.
  location = "global"

  depends_on = [google_project_service.mesh]
}

resource "google_gke_hub_feature_membership" "mesh" {
  count = var.enabled ? 1 : 0

  project    = var.project_id
  location   = "global"
  feature    = google_gke_hub_feature.mesh[0].name
  membership = local.membership_id

  mesh {
    # AUTOMATIC: Google picks and upgrades the control-plane revision. The
    # alternative, MANAGEMENT_MANUAL, means pinning a revision and doing
    # canary upgrades of the control plane by hand — which is work this project
    # has no reason to take on, since the cluster is rebuilt every session and
    # never outlives a revision.
    management = "MANAGEMENT_AUTOMATIC"
  }
}
