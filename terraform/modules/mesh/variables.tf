variable "project_id" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "cluster_location" {
  description = "Region for a regional cluster. Part of the fleet membership's cluster URI."
  type        = string
}

variable "existing_membership_id" {
  description = <<-EOT
    Set this when the cluster is ALREADY registered in the fleet — which is the
    case on this project: `insurance-gke` was registered by hand before this
    module existed, and is therefore not in Terraform state.

    Left null, the module creates its own membership. Pointed at an existing
    one, it only attaches the servicemesh feature and leaves the membership
    alone. Without this the module would try to create a *second* membership
    for the same cluster.

    The right long-term fix is to import the existing membership:

      terraform import 'module.mesh.google_gke_hub_membership.cluster[0]' \
        projects/<project>/locations/global/memberships/insurance-gke

    after which this can go back to null and the membership is managed here —
    and, per §11, actually removed by `terraform destroy` instead of surviving
    it.
  EOT
  type        = string
  default     = null
}

variable "injection_revision" {
  description = <<-EOT
    The revision label value for sidecar injection, used only to build the
    `namespace_label_command` output.

    Not hardcoded, because it is not stable: it depends on which release
    channel the managed control plane is on. This cluster reports
    `asm-managed-rapid` on its injected pods (rapid channel); the default
    channel is `asm-managed` and stable is `asm-managed-stable`. Labelling a
    namespace with the wrong one silently produces no sidecar at all.

    Read the live value from an already-injected pod rather than guessing:

      kubectl get pod -n api-serving-ns <pod> -o jsonpath='{.metadata.labels.istio\\.io/rev}'
  EOT
  type        = string
  default     = "asm-managed-rapid"
}

variable "enabled" {
  description = <<-EOT
    Whether to stand the mesh up at all.

    A switch rather than an unconditional resource because the mesh is only
    needed from build step 8 onward (§18) and the sidecar it injects into every
    pod in a labelled namespace costs CPU and memory on an Autopilot cluster
    that bills per pod. Sessions working on the batch or streaming steps can
    leave it off.
  EOT
  type        = bool
  default     = true
}
