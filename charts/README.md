# Kubernetes workloads

Part 1's `docker-compose.yml` orchestrated every service on one host. On GKE the
container images stay — that is what Kubernetes runs — but the orchestration moves
here. Each Compose service becomes a Deployment, StatefulSet, or Job.

| `docker-compose.yml` service | Here | Step |
|---|---|---|
| `postgres` | `postgres/` — StatefulSet + PVC, official image | 2 |
| `airflow-init`, `airflow-permissions` | `airflow/` — migration and create-user Jobs | 2 |
| `airflow-webserver` | `airflow/` — Deployment | 2 |
| `airflow-scheduler` | `airflow/` — Deployment, KubernetesExecutor | 2 |
| `clickhouse` | `clickhouse/` — StatefulSet + PVC | 2 |
| — (new in Part 2) | `mlflow/` — Deployment + Service in `ml-ns` | 6 |
| — (new in Part 2) | `kubeflow/` — kustomize overlay, 12 Deployments in `ml-ns` | 6 |
| `kafka` | not yet | 3 |
| `flink-jobmanager`, `flink-taskmanager` | not yet | 3 |
| `opensearch`, `datahub-*` | not yet | 4 |
| bind-mounted data dirs | GCS (`jobs/lakehouse.py`) | 2 |
| `depends_on` | readiness probes | 2 |

## Deploy

Requires the Step 1 infrastructure (`terraform/`) and the `data-ns` namespace with
the Workload-Identity-annotated `airflow` ServiceAccount, which
`terraform/verify.sh` creates.

```bash
# 1. Secrets first — the charts reference them by name
./charts/bootstrap-secrets.sh data-ns

# 2. Data stores
helm upgrade --install postgres   charts/postgres   -n data-ns --wait
helm upgrade --install clickhouse charts/clickhouse -n data-ns --wait

# 3. Airflow — official chart, our values
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.15.0 -n data-ns -f charts/airflow/values.yaml --wait --timeout 15m

# 4. MLflow — tracking server + model registry (step 6)
helm upgrade --install mlflow charts/mlflow -n ml-ns --wait

# 5. Kubeflow Pipelines — CRDs first, then the control plane (step 6)
kubectl apply -k charts/kubeflow/cluster-scoped
kubectl wait --for=condition=established --timeout=60s \
  crd/workflows.argoproj.io crd/scheduledworkflows.kubeflow.org
kubectl apply -k charts/kubeflow
```

The two-phase KFP apply is not optional: the workflow-controller crashloops if
`workflows.argoproj.io` is not established before it starts. `charts/kubeflow` is
the one kustomize overlay among the Helm charts — KFP publishes no chart, and
Argo CD reads kustomize natively, so §8 is unaffected. It carries seven patches
against upstream; `charts/kubeflow/kustomization.yaml` explains each, and four of
them are upstream bugs rather than customization.

`bootstrap-secrets.sh` also creates `ml-ns`, the `mlflow` and `training`
ServiceAccounts (annotated for Workload Identity from `terraform output`), and the
`mlflow-db` Secret. MLflow's backend store is a separate `mlflow` database on the
same Postgres, created by an init container on first start.

### Secrets

`bootstrap-secrets.sh` generates the database password, the Airflow metadata
connection string, and the webserver session key, and applies them as cluster
Secrets. No value is committed. Re-run it after every `terraform destroy` — the
cluster and everything in it is gone.

It deliberately reuses an existing Postgres password rather than rotating it: the
official image only applies `POSTGRES_PASSWORD` when initialising an empty data
directory, so a fresh password against an existing PVC would leave the database
on the old one and Airflow unable to authenticate, with nothing in the error
pointing at the cause.

## Access

```bash
kubectl port-forward -n data-ns svc/airflow-webserver 8080:8080   # admin/admin
kubectl port-forward -n data-ns svc/clickhouse 8123:8123
kubectl port-forward -n ml-ns svc/mlflow 5000:5000                # MLflow UI
kubectl port-forward -n ml-ns svc/ml-pipeline-ui 3000:80          # Kubeflow UI
kubectl port-forward -n ml-ns svc/ml-pipeline 8888:8888           # KFP API
```

The Kubeflow UI is reached by port-forward rather than the gateway on purpose:
§10 puts nginx in front of Grafana, Loki, Tempo and `fraud-prediction-api`. The
KFP control plane is an operator tool, not part of the serving surface, and
exposing an unauthenticated pipeline API through the gateway would be a
regression against §14.

MLflow 3 validates the Host header against an allow-list (DNS-rebinding
protection). `localhost` is included, so port-forwarding works; in-cluster DNS
names had to be added explicitly — see `charts/mlflow/values.yaml`.

## Training a model

```bash
kubectl delete job fraud-training -n ml-ns --ignore-not-found
kubectl apply -f ml/training-job.yaml
kubectl logs -n ml-ns -l job-name=fraud-training -f
```

The run registers a new `fraud-detector` version and tags it with the Delta
`data_version` it read. It never promotes — see [`docs/ml.md`](../docs/ml.md).

The Job is still the fastest way to run training alone. The same two steps as a
Kubeflow pipeline, with the run visible in the UI:

```bash
kubectl port-forward -n ml-ns svc/ml-pipeline 8888:8888 &
.venv-ml/bin/python -m ml.submit --wait
```

`ml/submit.py` recompiles `ml/pipeline.yaml` from `ml/pipeline.py` before
submitting, so a run cannot execute a spec that has fallen behind its source, and
groups runs under the `insurance-fraud` experiment — the same name as the MLflow
experiment, so a KFP run and its MLflow run are findable from each other.

## Why these choices

**Airflow chart pinned to 1.15.0.** Its default appVersion is 2.9.3, matching the
image exactly. Chart 1.17+ defaults to Airflow 3.x, where `schedule_interval` and
`airflow.operators.bash` both changed — the existing DAG would not import.

**KubernetesExecutor, not Celery.** One pod per task. This is also why the batch
jobs had to move off local disk first: under Celery a shared worker filesystem
would let the old local-path handoffs keep working by accident, hiding the
problem rather than fixing it.

**All Airflow components run as the existing `airflow` KSA.** That is the
identity Terraform granted `roles/iam.workloadIdentityUser` to. Letting the chart
create per-component service accounts would produce identities with no GCS
access, failing at the first Spark read with a confusing permission error.

**Own Postgres instead of the chart's bundled subchart.** The subchart pulls
`docker.io/bitnami/postgresql`, and Bitnami retired their free Docker Hub images —
chart 1.15.0's pinned tag now fails outright:

```
failed to resolve reference "docker.io/bitnami/postgresql:16.1.0-debian-11-r15": not found
```

Bitnami's `bitnamilegacy` namespace still has it, but it is a frozen archive they
reserve the right to delete, so pointing there would only defer the same break.
`charts/postgres/` uses the official maintained image, which is also what
CLAUDE.md specifies.

**Single-node ClickHouse, no operator.** Gold is ~58k rows. A cluster would add
Keeper, replication config, and an operator to manage them for no measurable gain.

**Redis, Flower, triggerer, StatsD, pgbouncer all disabled.** Redis and Flower
serve CeleryExecutor. There are no deferrable operators, so the triggerer would be
an idle billed pod. Metrics come from Prometheus in §12, not StatsD.

## Known gap: task logs

Task logs live only in the executor pod, which Kubernetes deletes when the task
finishes, so the Airflow UI reports missing logs for completed tasks. Read them
live instead:

```bash
kubectl logs -n data-ns -l dag_id=insurance_batch_bronze_silver_gold -f --max-log-requests 10
```

Persisting them needs either a ReadWriteMany volume (Filestore on Autopilot, not
cheap) or GCS remote logging via `apache-airflow-providers-google`, which is not
in the image yet. GCS remote logging is the intended fix and belongs with the §12
observability work.

## Troubleshooting

**`kubectl` times out with `dial tcp <ip>:443: i/o timeout`.** GKE can rotate the
control-plane endpoint during automatic maintenance, which leaves the cached
kubeconfig pointing at the old address. The cluster is fine; re-fetch credentials:

```bash
gcloud container clusters get-credentials insurance-gke \
  --region northamerica-northeast1 --project aide-playground
```

Confirm the endpoint really moved with
`gcloud container clusters list --format="value(name,status,endpoint)"` before
assuming anything is broken.

**`publish_datahub_lineage_stub` fails with `Failed to resolve 'datahub-gms'`.**
Expected until Step 4 — DataHub is not deployed yet. Two things need doing when it
is: deploy `datahub-gms`, and point `SILVER_QUALITY_REPORT_PATH` at the GCS report
(`gs://<bucket>/reports/silver_quality_report.json`), since it still defaults to a
local path that no longer exists in a task pod.

## Parking the platform between sessions

GKE has no stop/start — a cluster bills for as long as it exists. But Autopilot
charges for *pod resource requests*, so scaling every workload to zero removes the
compute cost and lets Autopilot drain the nodes.

```bash
./charts/scale.sh down     # end of session
./charts/scale.sh up       # next session
./charts/scale.sh status
```

What still bills while parked: the cluster management fee (normally offset by
GKE's free-tier credit) and the PersistentVolumes — a few cents a day for 20 GiB.

Parking is worth it because the PVCs keep ClickHouse's Gold tables, Airflow's
metadata and DAG history, and Redis's materialized features, so resuming is a
scale-up rather than a ~35-minute rebuild (apply, secrets, three helm installs,
Bronze upload, batch DAG, materialize).

Prefer `terraform destroy` when stopping for more than a few days — it ends the
cluster fee and disk charges too. Remote state survives in the bootstrap bucket, so
coming back is `terraform apply` plus `bootstrap-secrets.sh`.
