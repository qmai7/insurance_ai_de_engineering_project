# Platform configuration. Committed on purpose — nothing here is secret, and
# pinning it keeps every session reproducible.

project_id = "aide-playground"
region     = "northamerica-northeast1"

# The only repo the GitHub Actions Workload Identity Federation pool trusts.
github_repository = "qmai7/insurance_ai_de_engineering_project"

# Public nodes: no Cloud NAT charges. See variables.tf for the full rationale.
enable_private_nodes = false

# Kubernetes ServiceAccounts allowed to reach the lakehouse bucket.
# Extend this list as later build steps add workloads.
workload_identity_bindings = [
  "data-ns/airflow",
  "data-ns/spark",
  # MLflow's artifact store is the same lakehouse bucket, so the tracking server
  # and the training jobs both need the bucket-scoped GSA. Separate KSAs rather
  # than one shared identity: the server and a training run are different
  # workloads, and per-workload KSAs are what makes a future least-privilege
  # split a config change instead of a redeployment.
  "ml-ns/mlflow",
  "ml-ns/training",
  # Kubeflow Pipelines runs every step pod under its own `pipeline-runner` KSA,
  # not the one the equivalent kubectl Job uses — so without this binding the
  # training step fails at the first Feast read with a 403, while the identical
  # code succeeds as a Job. It cannot simply reuse `ml-ns/training`: KFP's
  # launcher also needs the RBAC that ships attached to `pipeline-runner`.
  "ml-ns/pipeline-runner",
]
