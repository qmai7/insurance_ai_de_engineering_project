# Platform configuration. Committed on purpose — nothing here is secret, and
# pinning it keeps every session reproducible.

project_id = "aide-playground"
region     = "northamerica-northeast1"

# Public nodes: no Cloud NAT charges. See variables.tf for the full rationale.
enable_private_nodes = false

# Kubernetes ServiceAccounts allowed to reach the lakehouse bucket.
# Extend this list as later build steps add workloads.
workload_identity_bindings = [
  "data-ns/airflow",
  "data-ns/spark",
]
