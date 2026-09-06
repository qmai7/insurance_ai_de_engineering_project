# CLAUDE.md — Insurance Fraud Detection ML Platform (Part 2)

> Instructions for Claude Code. Category headers match the instructor's rubric
> (`Coursework_Tracking__Public__1_.xlsx`, sheet "rubic final-coursework").
> Part 1 details live in root `README.md` — don't restate, read it.

---

## 0. Context

- Binary fraud classification (`is_fraud`, `claim_id` grain, synthetic label).
- **Part 1 is being re-platformed, not just extended**: everything that ran locally
  (Airflow, Spark, Flink, Kafka, ClickHouse, DataHub, storage) moves onto GCP/GKE.
  There is no local/cloud split anymore — it's all cloud.
- Cluster is ephemeral ($422 GCP credit) — spin up per work session, `terraform destroy`
  tears down everything including GCS + IAM. Nothing created outside Terraform state.
- **Build one step at a time — see Section 18 for the order.** Don't scaffold later
  steps' infrastructure while an earlier step is still unverified.

### Locked decisions

| Decision | Why |
|---|---|
| Single ephemeral **GKE Autopilot** cluster, one cluster, namespace-scoped | short bursty sessions suit per-pod billing; a 2nd cluster/control-plane buys no isolation a namespace doesn't already give |
| Namespaces: `data-ns` (Airflow, Kafka, Flink, ClickHouse, DataHub, Postgres), `ml-ns` (MLflow, Kubeflow), `api-serving-ns` (FastAPI fraud-prediction-api, drift-api, **Redis**), `kserve-ns` (KServe InferenceServices), `istio-system` (mesh control plane), `gateway-ns` (nginx ingress, cert-manager), `argocd-ns` (Argo CD), `vault-ns`(HashiCorp Vault), `monitoring-ns` (Grafana,Promethus,Tempo,Loki) | maps cleanly to rubric categories; keeps CI/CD and ArgoCD apps scoped per concern |
| **GCS** replaces MinIO for Bronze/Silver/Gold | one less stateful service; native Spark/Flink/Feast connectors via Workload Identity |
| **KServe + Istio** (not Kourier) | Istio VirtualService/DestinationRule = real canary traffic-splitting; Istio mTLS also covers the separate Security rubric item |
| Redis lives in `api-serving-ns` | colocated with the service that reads it synchronously on the hot path |
| Postgres (Airflow + MLflow metadata): in-cluster StatefulSet, not Cloud SQL | free, torn down with the cluster; metadata is regenerable per session |
| ClickHouse stays the Gold analytics warehouse; a Spark job exports Gold → GCS Parquet as the Feast offline source | avoids the unstable community ClickHouse-Feast connector |
| Data-pulling/prediction API is called **`fraud-prediction-api`** | it returns a prediction, not raw features |
| `drift-api`: plain FastAPI Deployment, no KServe/KNative | same rubric line satisfied, simpler shape |
| Feature-store has **3 separate CI/CD'd jobs** — don't collapse them (see Section 3) | Materialize Pipeline (batch, offline→online) ≠ Job 1 (streaming→offline) ≠ Job 2 (streaming→online) |
| Data versioning: **Delta snapshots**, see Section 7 | reuses Silver's existing Delta infra, no new tool |
| Terraform provisions everything (cluster, GCS, IAM) | `terraform destroy` leaves zero orphaned billing |

### Open decisions
1. **Model promotion**: manual vs. automated MLflow-registry watcher. Diagram the automated
   loop, implement manual first, call out auto-promotion as a stretch goal.

---

## 1. Web API kéo dữ liệu — `fraud-prediction-api`

Pulls features from Redis by ID, forwards to KServe for scoring, returns the result.
Pydantic validation, `/healthz`+`/readyz`, fully async, Helm `--atomic` rollout with a
captured rollback demo, behind the gateway (basic auth + rate limit + HTTPS/domain),
KEDA autoscale on request rate.

## 2. Web API cho Real-time Drift Detection — `drift-api`

Same requirements as Section 1 (pydantic, healthcheck, async, `--atomic`, KEDA). Plain
FastAPI Deployment. Fired async by fraud-prediction-api after each prediction.

## 3. Feature Store

Three separate pipelines, each its own CI/CD:
1. **Materialize Pipeline** (Airflow, `feast materialize-incremental`) — batch, GCS/Parquet → Redis.
2. **Job 1** — push Flink's streaming feature output into the **offline** store (GCS/Parquet).
   Only push columns that are actually part of the model's feature set.
3. **Job 2** — push the same streaming features into the **online** store (Redis) directly,
   via Feast's push API.

**Why both writes**: Job 2 keeps Redis fresh for serving; Job 1 is what makes a streaming
feature *trainable* — Redis entries expire, so without an offline record that feature could
never be joined against labels for training, and a model using it in production would be
scoring on a signal it never saw in training (train/serve skew).

Define a TTL per feature table with a written rationale (e.g. short TTL for `feat_stream_30m`
since it's near-real-time and goes stale fast; longer for `feat_customer_90d`).

## 4. ML

A Jupyter notebook first, before any pipeline: load features via the Feast SDK, merge with
the label table (`claim_id`, `is_fraud`), train/val split, train, evaluate, save `.joblib`.
Graded as its own deliverable — don't skip to Kubeflow.

## 5. ML Pipelines

Same steps as the notebook, as a Kubeflow pipeline, plus a distributed training step
(e.g. distributed XGBoost). Every run logged to MLflow.

## 6. Improve the Data Generator

Simulate data drift (configurable). Generate a label table: `claim_id`, `is_fraud`.

## 7. Versioning

- **Model**: MLflow registry — Postgres for run metadata, GCS for artifacts. The registry's
  `Production` pointer is what KServe reads to pull the model's GCS URI at serving time.
- **Data — easiest option, no new tool**: the Gold→GCS export writes a **Delta table**
  instead of plain Parquet. Delta already versions every write automatically via its
  transaction log — nothing extra to build. To version a training pull, just read a specific
  version and log the number as an MLflow tag:
  ```python
  df = spark.read.format("delta").option("versionAsOf", version_num).load(gcs_path)
  mlflow.set_tag("data_version", version_num)
  ```
  Feast keeps reading its own plain-Parquet export as before, unchanged — the Delta table is
  a parallel, training-pipeline-only layer, so there's no Feast/Delta compatibility question
  to resolve.

## 8. CI/CD

GitHub Actions build/test (unit test + lint)/push to **Google Artifact Registry**
(`<region>-docker.pkg.dev/<project>/insurance-images`, provisioned by Terraform).
AR needs no PAT to push (Docker auths
via gcloud) and no `imagePullSecret` to pull (the GKE node SA is granted
`artifactregistry.reader` on the repo), so it removes two hand-managed
credentials. ArgoCD app-of-apps syncs from `charts/`. Separate
pipeline per: Materialize Pipeline, Training Pipeline, Airflow pipelines, fraud-prediction-api (done in section 10), drift-api(done in section 10). Pipeline-time secrets (GitHub Actions) stay
separate from runtime secrets (Vault).

## 9. Validation & Verification

Unit tests >90% coverage with fixtures/mocks. Equivalence partitioning + boundary value
analysis for test design. Mutation testing (`mutmut`, >80% score, **changed code only per
push**). Property-based idempotency testing (`hypothesis`, optional `crosshair`) — e.g.
repeated calls to the model give consistent predictions. Load testing (`locust`) against
fraud-prediction-api, HTML report as the SLA artifact.

## 10. Routing & Gateway

nginx ingress in front of: Grafana, Loki, Tempo, fraud-prediction-api. Basic auth + rate
limit + real domain/HTTPS specifically on fraud-prediction-api.

## 11. IaC

Terraform only (Ansible dropped) — cluster, GCS, IAM, organized per-service under `terraform/`.

## 12. Observability

fraud-prediction-api metrics (req/s, count, failures) + infra telemetry via Prometheus/Grafana.
Logs via Loki, traces via Tempo (request ID propagated through to KServe).

ML telemetry — two mechanisms:
1. Periodic Airflow DAG: pull offline features, compute drift (no ground truth available),
   push to Grafana via **Prometheus Pushgateway** (Airflow tasks are too short-lived to scrape
   directly). Last step calls the Kubeflow API to trigger retraining past a threshold.
2. Real-time `drift-api`, fired per-prediction.

## 13. A/B Testing

Champion vs. challenger `InferenceService`, Istio-split traffic, staged ramp (10→25→50→100%).
No ground truth at request time — compare via proxy metrics in Grafana: prediction-distribution
drift (PSI/KS) between models, disagreement rate, fraud-flag rate over time, latency/error
rate per version.

## 14. Security

Vault for runtime secrets only. Istio mTLS for service-to-service auth (this is the same
KServe+Istio decision satisfying a second rubric line). Never plaintext secrets in Airflow.

## 15. Repository Design

Repository pattern for data access (Feast/ClickHouse/GCS clients behind thin interfaces).
Clear separation: FastAPI request layer / business logic / KServe client. 2-3 named
patterns, not a full DDD architecture.

## 16. Documentation

Docs in `docs/`, linked from `README.md`. Low-level design doc: 5 key classes for the ML
focus (e.g. `TrainingDataService`, `SplitService`, per the rubric's example shape).

## 17. Novel ideas

Two ideas, documented with proof — not yet chosen.

---

## 18. Build order — one step at a time

Each step must be deployed and **tested working by the user** before starting the next.
Don't pre-build later steps' infra while an earlier step is unverified.

1. **Terraform**: GKE Autopilot cluster + GCS bucket + IAM only. Verify cluster is reachable.
2. **Part 1 batch pipeline** on GCP: Airflow + Spark + ClickHouse (DP1/DP2/DP3), storage on
   GCS instead of local disk. Test the existing batch DAG runs end-to-end on GKE.
3. **Part 1 streaming pipeline** on GCP: Kafka + Flink, JSONL replay (no CDC). Test the
   existing streaming job runs end-to-end on GKE.
4. **DataHub** on GCP, lineage reconnected to the migrated pipelines. Test lineage renders
   correctly for both batch and streaming.
5. **Feature store**: Gold→GCS Delta export, Materialize Pipeline, Job 1, Job 2. Test Redis
   gets populated and matches the offline snapshot.
6. **ML notebook**, then the Kubeflow training pipeline + MLflow. Test a model lands in the
   registry with logged data version.
7. **fraud-prediction-api + drift-api**, plain deployments first (no KServe yet). Test
   end-to-end prediction requests against a locally-served model.
8. **KServe + Istio**, canary traffic split. Test champion/challenger split works.
9. **Gateway, observability, security, A/B dashboards, CI/CD wraps around each step above**.

When starting a session, check which step is currently in progress before writing any code.
