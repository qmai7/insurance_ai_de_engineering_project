# Helm and Kubernetes deployment charts

The `charts/` directory is the deployment layer for the GKE platform. It
replaces the orchestration role that `docker-compose.yml` had locally in part 1: the
container images remain the application runtime, while Helm charts and
Kustomize manifests describe how those images run as Kubernetes workloads.

Terraform creates the cluster, namespaces' infrastructure, GCS, IAM and
Artifact Registry. This directory creates the in-cluster services that consume
that infrastructure.

## 1. The chart layout

| Path | Deployment mechanism | Namespace | Main responsibility |
|---|---|---|---|
| `charts/postgres/` | Local Helm chart | `data-ns` | Stateful PostgreSQL metadata store for Airflow and MLflow |
| `charts/clickhouse/` | Local Helm chart | `data-ns` | Gold analytics warehouse |
| `charts/airflow/values.yaml` | Values for the upstream Airflow chart | `data-ns` | Airflow webserver, scheduler, KubernetesExecutor task pods |
| `charts/mlflow/` | Local Helm chart | `ml-ns` | MLflow tracking server and model registry |
| `charts/redis/` | Local Helm chart | `api-serving-ns` | Feast online feature store |
| `charts/fraud-prediction-api/` | Local Helm chart | `api-serving-ns` | The prediction API ([`docs/api.md`](api.md)) |
| `charts/model-server/` | Local Helm chart | `api-serving-ns` | Champion + challenger model serving and the mesh routing rules ([`docs/service_mesh.md`](service_mesh.md)) |
| `charts/kubeflow/` | Kustomize overlay | `ml-ns` | Kubeflow Pipelines control plane |
| `charts/argocd/` | Values for the Argo CD chart | `argocd-ns` | GitOps controller configuration |
| `charts/argocd-apps/` | Argo CD `Application` manifests | Argo CD control plane | App-of-apps registration for each service |
| `charts/bootstrap-secrets.sh` | Kubernetes bootstrap script | `data-ns`, `ml-ns` | Creates namespaces, ServiceAccounts and runtime Secrets |

The local charts have the usual Helm shape:

```text
charts/<service>/
├── Chart.yaml       chart identity and version
├── values.yaml      configurable defaults
└── templates/       Deployments, StatefulSets, Services, Jobs, PVCs, RBAC
```

`Chart.yaml` identifies a chart. `values.yaml` holds deployment choices that
vary by environment, such as image tags, resource requests and storage sizes.
`templates/` turns those values into Kubernetes manifests. Helm renders the
chart before applying the manifests, so Kubernetes receives ordinary YAML and
does not need Helm-specific runtime components.

## 2. What runs where

### `data-ns`: data and orchestration

- **Postgres** is a StatefulSet with persistent storage. Airflow uses its
  metadata database, and MLflow uses a separate database on the same Postgres
  instance.
- **ClickHouse** is a deliberately small, single-node StatefulSet for Gold
  analytics. A ClickHouse cluster would add Keeper, replication and operator
  overhead that the current dataset does not need.
- **Airflow** uses the official Apache Airflow chart, pinned to chart `1.15.0`
  and Airflow `2.9.3`. The chart's values point both the scheduler/webserver
  and KubernetesExecutor task pods at the same `airflow-spark` image, which
  contains the DAGs and `jobs/` code.

Airflow uses `KubernetesExecutor`, so each task becomes its own pod. This makes
resource isolation explicit and removes the accidental shared filesystem that
local Docker Compose could provide. The pod uses the existing `airflow`
Kubernetes ServiceAccount, which Terraform authorizes to use the platform data
identity and read/write the GCS lakehouse.

### `ml-ns`: machine learning control plane

- **MLflow** provides experiment tracking, model artifacts and the model
  registry. Its backend metadata is Postgres; model artifacts are stored in
  GCS.
- **Kubeflow Pipelines** runs the six-step training graph: build dataset,
  split by time, train, evaluate, log/register in MLflow, and quality gate.
  KFP's own internal bookkeeping is separate from MLflow's metadata.

### `api-serving-ns`: online feature serving and prediction

**Redis** is colocated with the API-serving namespace because
`fraud-prediction-api` reads online features synchronously on its hot path.
Feast materializes features into this Redis instance; the offline source
remains Parquet in GCS.

- **fraud-prediction-api** takes a claim ID, reads that Redis, and returns a
  decision.
- **model-server** holds the model and answers with a probability. It is
  rendered twice — champion and challenger — behind one Service, with the
  traffic split between them owned by the mesh.

Both are in this namespace and not their own because the API's Redis read is
on the same hot path, and because a single namespace is the injection unit for
the service mesh: this is the only namespace labelled for sidecar injection.
See [`docs/api.md`](api.md) and [`docs/service_mesh.md`](service_mesh.md).

## 3. Helm and Kustomize roles

Most services use Helm because they are naturally parameterized releases:
image repository/tag, resource requests, PVC sizes, database settings and
ServiceAccounts can be expressed as values and rendered consistently.

Airflow is a **multi-source Argo CD Application** rather than a copied local
chart. Argo CD pulls chart `1.15.0` from the Apache Airflow repository and
layers this repo's `charts/airflow/values.yaml` on top. That keeps upstream
chart maintenance upstream while keeping project-specific configuration in
this repository.

Kubeflow is the exception. KFP publishes no Helm chart, so
`charts/kubeflow/` is a Kustomize overlay over the pinned upstream KFP `2.17.0`
manifests. Its patches fix namespace assumptions, remove plaintext upstream
credentials, set Autopilot-sized resources and adjust the object store and
metadata database configuration.

KFP must be applied in two phases:

1. Apply `charts/kubeflow/cluster-scoped/` so the Workflow and ScheduledWorkflow
   CRDs exist.
2. Wait until those CRDs are `Established`.
3. Apply the namespaced `charts/kubeflow/` overlay so the controller can start
   against already-registered CRDs.

Starting the controller before that wait causes the workflow controller to
crashloop. Argo CD represents this ordering with separate Applications and
sync waves rather than treating KFP as an ordinary single Helm release.

## 4. Secrets and identities

Secrets are deliberately not values committed to git. Run
`charts/bootstrap-secrets.sh` after creating a cluster and after every
`terraform destroy`:

- reuses the existing Postgres password when a PVC already exists
- creates the Airflow metadata connection and webserver session Secret
- creates MLflow and KFP credentials required by their charts
- creates the `mlflow`, `training` and other ServiceAccounts expected by the
  deployments
- attaches the Workload Identity annotations that let pods access GCS without
  JSON key files

The charts refer to Secret names and keys, not plaintext values. This keeps
runtime credentials out of Helm values, git history and rendered pipeline
configuration. It also means the bootstrap script must run before charts that
reference those Secrets are synced.

## 5. How a deployment flows

The high-level flow is:

```text
Terraform
  └─ GKE + GCS + IAM + Artifact Registry
       └─ bootstrap-secrets.sh
            └─ namespaces, ServiceAccounts and Secrets
                 └─ Argo CD root Application
                      └─ child Applications in charts/argocd-apps/
                           ├─ Helm renders local charts
                           ├─ upstream Airflow chart + local values
                           └─ Kustomize renders Kubeflow overlay
                                └─ Kubernetes Deployments, StatefulSets, Jobs, Services, PVCs
```

For a normal Airflow code change:

1. A change under `dags/`, `jobs/` or the Airflow Dockerfile triggers the
   Airflow GitHub Actions workflow.
2. CI runs lint and tests, builds the `airflow-spark` image with the commit SHA
   as its tag, and pushes it to Artifact Registry using Workload Identity
   Federation.
3. CI commits the new tag into `charts/airflow/values.yaml`.
4. Argo CD notices the git change, renders the Airflow Application and updates
   the relevant Kubernetes Deployments.
5. GKE pulls the image using the node service account's Artifact Registry read
   permission; no `imagePullSecret` is needed.

Git is the handoff between CI and
Argo CD; Argo CD is the component that changes the cluster.

## 6. Design choices

**Official Airflow chart, pinned version.** Chart `1.15.0` uses Airflow `2.9.3`,
which matches the image and the DAG API. Moving to a chart that defaults to
Airflow 3 would change APIs used by the existing DAGs.

**Own Postgres chart.** The project uses the maintained official Postgres image
instead of the Airflow chart's retired Bitnami dependency. This keeps the
metadata store aligned with the platform's locked Postgres decision.

**Single-node ClickHouse.** Gold is small enough that a replicated ClickHouse
cluster would add operational cost without useful capacity or resilience for
this ephemeral coursework cluster.

**GCS instead of local bind mounts.** Bronze, Silver and Gold data survive pod
boundaries in GCS. Kubernetes tasks are disposable, so no job depends on a
previous task's local filesystem.

**Argo CD instead of direct CI deployment.** CI produces an image and a git
change; Argo CD continuously compares git-rendered manifests with the live
cluster and reconciles the difference. This preserves auditability, supports
recovery after `terraform destroy`, and avoids placing cluster credentials in
GitHub Actions.

## 7. Access and troubleshooting

The operational UI surfaces are intentionally port-forwarded rather than
exposed through the application gateway:

```bash
kubectl port-forward -n data-ns svc/airflow-webserver 8080:8080
kubectl port-forward -n ml-ns svc/mlflow 5000:5000
kubectl port-forward -n ml-ns svc/ml-pipeline-ui 3000:80
kubectl port-forward -n argocd-ns svc/argocd-server 8082:80
```

Check rendered/live workloads with:

```bash
kubectl get pods -n data-ns
kubectl get pods -n ml-ns
kubectl get applications.argoproj.io -n argocd-ns
helm list -A
```

If the GKE API endpoint has rotated, refresh local credentials before
troubleshooting Helm or Kubernetes:

```bash
gcloud container clusters get-credentials insurance-gke \
  --region northamerica-northeast1 --project aide-playground
```

For the lower-level deployment commands and chart-specific troubleshooting,
see [`charts/README.md`](../charts/README.md). For the GitOps workflow, see
[`docs/cicd.md`](cicd.md).
