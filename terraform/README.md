# Infrastructure (Terraform)

Part 2 re-platforms Part 1 onto GCP. Every billable resource is created here, so
`terraform destroy` is guaranteed to leave nothing behind — the cluster is
ephemeral by design and spun up per work session against a fixed GCP credit.

## Layout

```text
terraform/
├── bootstrap/          # One-time: the GCS bucket holding remote state (local state)
├── versions.tf         # Provider pins + gcs backend
├── variables.tf        # All inputs, each with its rationale
├── terraform.tfvars    # Pinned values (committed — nothing secret)
├── main.tf             # Project APIs + module wiring
├── outputs.tf
└── modules/
    ├── network/        # VPC, subnet with secondary ranges, optional Cloud NAT
    ├── gke/            # Regional Autopilot cluster
    ├── storage/        # Lakehouse bucket (replaces Part 1's MinIO)
    └── iam/            # Data-plane SA + Workload Identity bindings
```

## What Step 1 creates

| Resource | Name | Note |
|---|---|---|
| VPC + subnet | `insurance-vpc` / `insurance-subnet-northamerica-northeast1` | Named secondary ranges for Pods and Services |
| GKE Autopilot | `insurance-gke` | Regional, `REGULAR` channel, public nodes |
| GCS bucket | `aide-playground-lakehouse` | Prefix-separated layers, `force_destroy = true` |
| Service account | `insurance-data-platform@…` | Bucket-scoped `objectAdmin`, no JSON key ever issued |
| State bucket | `aide-playground-tfstate` | Created by `bootstrap/`, **survives destroy** |

Region is `northamerica-northeast1` (Montreal), matching the Canadian/Quebec
shape of the generated insurance dataset.

## First-time setup

Run once, ever. It creates the bucket that the main config stores its state in —
the chicken-and-egg that means this one step uses local state.

```bash
gcloud auth application-default login

cd terraform/bootstrap
terraform init && terraform apply
```

Never `terraform destroy` in `bootstrap/`. Deleting that bucket throws away the
state that tracks everything else, which orphans billable resources.

## Per-session lifecycle

```bash
# Spin up
cd terraform
terraform init            # first time per machine only
terraform apply

# Point kubectl at the cluster
$(terraform output -raw get_credentials_command)

# ... work ...

# Tear down — do this before closing the laptop
terraform destroy
```

Because state lives in GCS, `terraform apply` in a fresh session recreates the
cluster from scratch and the next session picks up cleanly.

## Design decisions worth knowing

**Autopilot over Standard.** Sessions are short and bursty, so per-pod billing
beats paying for idle nodes. The cost is losing `node_config` and custom node
pools, and having resource requests enforced — all acceptable here. Workload
Identity is always on, which is what the IAM module depends on.

**Public nodes, no Cloud NAT.** Private nodes cannot reach GHCR / PyPI / Maven
without a NAT gateway, and NAT bills hourly plus per-GB. This platform pulls a
lot of large images (Airflow, Spark, Flink, Kafka, ClickHouse, DataHub), so that
adds up against a fixed credit. Set `enable_private_nodes = true` to flip it —
the Cloud Router and NAT are provisioned automatically so egress keeps working
rather than silently breaking.

**Cloud Logging held to system components only.** The platform routes application
logs to Loki by design, so shipping workload logs to Cloud Logging as well would
mean paying per-GB for a duplicate copy. System-component logs stay on — they are
how you debug the cluster when nothing else is running yet.

**Managed Prometheus stays on, because Autopilot will not allow otherwise.** The
intent was to disable it for the same reason (self-hosted Prometheus is the
project's metrics story), but the GKE API rejects `managed_prometheus.enabled =
false` on Autopilot 1.25+ outright:

```
Error 400: Managed Service for Prometheus cannot be disabled in Autopilot
clusters version: 1.35.6-gke.1641000.
```

So GMP runs alongside self-hosted Prometheus. `enable_components` is still held
to `SYSTEM_COMPONENTS`, which *is* controllable and is the part that matters for
cost — GMP bills per sample ingested, and system metrics are a small fixed volume
next to scraping every workload pod.

**One bucket, prefixed by layer**, not three buckets. Bronze/Silver/Gold share
identical access rules and lifecycle, so separate buckets would add IAM surface
without buying isolation.

```text
gs://aide-playground-lakehouse/
├── bronze/   raw generated source data (Parquet offline, JSONL streaming)
├── silver/   cleaned, quality-gated Delta tables
├── gold/     Gold exports out of ClickHouse
├── feast/    Feast offline store (plain Parquet)
├── delta/    versioned Gold→Delta training snapshots
└── mlflow/   MLflow run artifacts and model files
```

**Object versioning off on the lakehouse, on for state.** Delta Lake's
transaction log already provides the data versioning the project needs, so GCS
versioning would bill for redundant copies of large Parquet files. State is the
opposite case: an interrupted apply can corrupt it, and versioning is the only
way back.

**`deletion_protection = false` on the cluster.** It defaults to true, which
would make `terraform destroy` fail — leaving a cluster billing overnight. That
failure mode is worse than the accident the flag protects against, given the
cluster holds nothing that isn't regenerable.

**No JSON service-account keys.** Pods authenticate by impersonating the GCP
service account through Workload Identity, getting short-lived credentials from
the metadata server. Nothing is mounted or committed.

## Connecting a workload to the lakehouse

Add the Kubernetes ServiceAccount to `workload_identity_bindings` in
`terraform.tfvars`, apply, then annotate the KSA:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow
  namespace: data-ns
  annotations:
    iam.gke.io/gcp-service-account: insurance-data-platform@aide-playground.iam.gserviceaccount.com
```

`terraform output ksa_annotation` prints that annotation line.

## Not created here (later steps)

Kubernetes namespaces, and every workload, arrive with Helm/ArgoCD in later build
steps — Terraform stays limited to GCP resources so there is no provider
chicken-and-egg between the cluster and things running inside it.
