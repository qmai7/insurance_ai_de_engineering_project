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
```

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
```

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
