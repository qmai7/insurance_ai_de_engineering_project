output "membership_id" {
  description = "Fleet membership the servicemesh feature is attached to."
  value       = var.enabled ? local.membership_id : null
}

output "membership_is_managed_here" {
  description = "False when attaching to a pre-existing, hand-registered membership — i.e. `terraform destroy` will NOT deregister the cluster from the fleet. See existing_membership_id for the import command that fixes it."
  value       = var.enabled && var.existing_membership_id == null
}

output "namespace_label_command" {
  description = <<-EOT
    Sidecar injection is opt-in per namespace and is NOT something Terraform
    does: the label goes on a Kubernetes object, and putting the kubernetes
    provider in this state would make `terraform destroy` depend on a reachable
    API server that the same destroy is tearing down.

    Only needed for a namespace that is not labelled yet. api-serving-ns on this
    cluster already carries the legacy `istio-injection=enabled` label and is
    injecting successfully — do not "upgrade" it to a revision label without
    checking `injection_revision` against a live pod, since a value that does
    not match the control-plane channel silently yields no sidecar at all.
  EOT
  value = var.enabled ? join(" && ", [
    for ns in ["api-serving-ns"] :
    "kubectl label namespace ${ns} istio.io/rev=${var.injection_revision} --overwrite"
  ]) : "mesh disabled"
}
