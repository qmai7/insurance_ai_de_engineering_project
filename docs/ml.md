# ML — baseline fraud model

The notebook is
[`notebooks/01_fraud_model_baseline.ipynb`](../notebooks/01_fraud_model_baseline.ipynb);
environment setup is in [`notebooks/README.md`](../notebooks/README.md). This page
records the decisions and the numbers, so neither has to be reverse-engineered from
cell outputs.

The notebook comes before the Kubeflow pipeline deliberately. A pipeline's value is
running a sequence repeatably, and there is nothing to gain from automating a
sequence nobody has watched work once — the point-in-time bug below is exactly the
kind of thing that survives being wrapped in a DAG.

## The path from data to model

```text
gs://…/feast/obt_claims_enriched   ─┐  entity spine: claim_id, customer_id, event_timestamp
                                    │
gs://…/bronze/offline/claim_labels ─┤  label: is_fraud
                                    │
                                    ├─► Feast get_historical_features()
                                    │      claim_features  (14, TTL 3650d)
                                    │      customer_90d    (5,  TTL 120d)
                                    ▼
                            2,700 rows × 19 features
                                    │  temporal split at the 80th percentile of claim_date
                                    ▼
                          LogisticRegression
                                    ▼
                        models/fraud_model.joblib
```

## Decisions

### Features come from Feast, not from SQL

The registry, the offline paths and the TTLs are all read from the same
`feature_store/` definition the Airflow Materialize Pipeline uses.

Entity keys and timestamps are the exception: `claim_id`, `customer_id` and
`event_timestamp` come from the Gold claims export, because the entity dataframe is
the *question* being asked of Feast, not an answer.

### Labels are read from Bronze, never from Gold

A label is not a feature. It arrives later, from a different process, and anything
placed in a feature view gets materialized into Redis where the prediction API
would happily read it. So `claim_labels` stays a separate table and is joined only here, in training.

### The split

Cut at the 80th percentile of `claim_date` rather than at random:

- production only ever scores the future; a random split lets the model learn from
  claims filed *after* the ones it is evaluated on
- the generator's drift window is the last 14 days (§6), and a random split spreads
  those rows across both halves.

The visible cost is that validation is harder than training by construction: the
fraud rate is **4.63% in train and 11.00% in validation**.

### Two features dropped at model level

| Dropped | Why |
|---|---|
| `claim_month` | Under a temporal split it is a proxy for the split. Train sees months 5–7, validation is month 8, so any learned weight is unusable by construction. `claim_day_of_week` and `claim_is_weekend` stay because they cycle. |
| `claim_status` | Excluded from the feature view itself, not just the model: it records the outcome of claim processing, decided *after* any fraud assessment. |

### Preprocessing lives inside the estimator

The saved artifact is a `sklearn.Pipeline` containing the `ColumnTransformer`, not a
bare booster. The prediction API reads raw feature values from Redis; expecting it
to reproduce an imputation strategy and a one-hot column order is train/serve skew
waiting for a deploy. Nulls in `risk_segment` become their own `"unknown"` category
rather than a filled-in guess — "we did not collect this" is information.

### One model

Logistic regression only. A boosted model was tried and scored within noise of it
(PR-AUC 0.329 vs 0.358) on a 582-row validation set with 64 positives, which is not
enough evidence to prefer either — so it was removed rather than kept as decoration.

The baseline mattering this much is itself informative: the generator's label *is* a
logistic function of a handful of features, so the ceiling here is near-linear by
construction. Real fraud is not, which is why the tree still belongs in the pipeline
— just not in this notebook.

## Results

582-claim validation set, in which 64 claims are fraud (11.0%)

**PR-AUC 0.358 · ROC-AUC 0.751**

The operating point is a **review budget, not a probability threshold**. Nobody
investigates every claim, so the question is what a team reviewing the top N% by
risk actually sees.

| Review budget | Flagged | Fraud caught | Precision | Recall | Lift vs random |
|---|---|---|---|---|---|
| 5% | 30 | 16 / 64 | 53.3% | 25.0% | 4.9× |
| **10%** | **59** | **21 / 64** | **35.6%** | **32.8%** | **3.2×** |
| 20% | 117 | 31 / 64 | 26.5% | 48.4% | 2.4× |
| 30% | 175 | 39 / 64 | 22.3% | 60.9% | 2.0× |

Which row to pick is a staffing decision, not a modelling one. The model supplies
the ordering; the budget decides where to cut.

### Reading these numbers correctly

**Accuracy is not reported, and precision is not accuracy.** A model that labels
every claim "legit" scores **89.0%** accuracy on this validation set and catches
zero fraud.

Precision at a budget is the number that matters: at 10%, roughly **1 flagged claim
in 3 is fraud, against 1 in 9 for random review**. The same investigator hours find
about three times as much fraud. Tighten to a 5% budget and it is better than a
coin flip.

### The ceiling is the data, not the algorithm

Three limits, in the order they cost the most. None is fixed by a different
estimator, which is the argument for keeping the model simple and spending the
effort on features:

1. **A real driver is unobservable.** The label generator uses the customer's true
   `risk_segment`, and ~2/3 of claims have it null because of the simulated schema
   change. No model recovers a column that is not there.
2. **64 positives in validation.** Wide error bars on everything above; a couple of
   claims crossing the threshold moves precision by several points. This is also
   why the boosted model proved nothing either way.
3. **Almost no behavioural history.** Most claimants have no prior claim in their
   90-day window, so the customer aggregates are mostly zero. Streaming features
   (`feat_stream_30m`) are the intended fix and need Kafka/Flink first.

## The bug this work exposed

Worth recording, because it was silent and the fix changed a Gold table.

`feat_customer_90d` was one snapshot per customer, every row stamped with the newest
claim date in the dataset. Every value in it was correct, and it was unusable for
training: a point-in-time join takes the newest feature row *at or before* the
entity's timestamp, so for a claim filed in May the only customer row available sat
in the future. Feast filtered it out — and dropped the entity row with it. The join
returned **zero rows**, not nulls.

The fix made the table a time series at `(customer_id, as_of_date)`, with as-of dates
drawn from every date a customer filed a claim (the training spine) plus the newest
claim date for every customer (the serving row Redis gets). Windows now end strictly
*before* `as_of_date`, which also closed a leak: the old snapshot's aggregates
included the very claim a model would be scoring. See
[`feature_store.md`](feature_store.md) and
[`data_modeling.md`](data_modeling.md#feature-store-design).

Two guards were added so it cannot come back quietly:

- a Gold quality gate on `(customer_id, as_of_date)` uniqueness — a duplicate pair
  would make Feast's tie-break arbitrary, since `created_timestamp` is identical
  across an export
- assertions in the notebook on **row count** and **per-column null rate** after
  retrieval. Row-count loss means a point-in-time miss; an all-null column means a
  view resolved but never matched. Neither raises on its own.

## From notebook to production path

The notebook is the exploration. `ml/` is the same model as deployable code, and
it is what §5 and §7 are built on. The two agree: the training job reproduces the
notebook's metrics exactly (PR-AUC 0.3577, ROC-AUC 0.7508), which is the only
useful proof that the productionised path did not quietly change the model.

```text
ml/config.py        the canonical feature spec, split policy, operating point
ml/repositories.py  one thin interface per external system (§15)
ml/services.py      the five services below (§16)
ml/train.py         wiring — build, split, train, evaluate, log, register
ml/gate.py          the pipeline's decision step
ml/promote.py       the manual promotion gate
ml/pipeline.py      the Kubeflow pipeline definition (§5)
```

### The five key classes (§16)

| Service | Responsibility |
|---|---|
| `TrainingDataService` | assemble a labelled, point-in-time-correct dataset; assert the join is intact |
| `SplitService` | divide it by time, refusing an empty or single-class side |
| `ModelBuilder` | construct the estimator with preprocessing inside it |
| `EvaluationService` | score it as ranking quality plus precision/recall at a review budget |
| `ModelRegistryService` | the only class that talks to MLflow: log, register, resolve, promote |

Data reaches them through four repositories — `ParquetClaimSpineRepository`,
`ParquetLabelRepository`, `FeastFeatureRepository`, `DeltaLogDataVersionRepository`
— declared as `Protocol`s. The point is testability, not layering for its own
sake: the first four services can be exercised with in-memory fixtures, no bucket
and no cluster, which is what makes §9's coverage target reachable.

`DeltaLogDataVersionRepository` deliberately reads the Delta version by listing
`_delta_log/*.json` rather than through Spark or `deltalake`. Pulling in a Delta
reader — and for Spark, a JVM — to learn one integer would make the training image
heavier than the model it produces.

## MLflow (§7)

Deployed by [`charts/mlflow`](../charts/mlflow) into `ml-ns`, image
[`dockerfile.mlflow`](../docker_image/dockerfile.mlflow).

- **Backend store** — the same in-cluster Postgres that backs Airflow, in its own
  `mlflow` database, created by an init container because the official Postgres
  image only creates `POSTGRES_DB` at initialisation.
- **Artifact store** — `gs://aide-playground-lakehouse/mlflow`, with artifact
  proxying **off**. Not a preference: §7 has KServe pull the model from GCS
  directly, and with proxying on MLflow records `mlflow-artifacts:/…` URIs that
  only the tracking server can resolve.
- **Identity** — `ml-ns/mlflow` and `ml-ns/training` KSAs, both impersonating the
  bucket-scoped GSA via Workload Identity. No key files.

### Aliases, not stages

§7 describes a `Production` *stage* pointer. **MLflow 3 removed model
stages**, so the pointer here is a registry **alias** named `production`. This is
a better fit anyway: an alias moves between versions atomically, so serving never
resolves to nothing mid-promotion, and a second alias is all §13's
champion/challenger ramp needs.

Resolving that alias to something KServe can fetch takes two non-obvious hops,
wrapped in `ModelRegistryService.aliased_model_location()`:

1. a `ModelVersion.source` is `models:/<logged-model-id>`, not a storage path
2. the id must be parsed out of that URI, because the registry response leaves
   `ModelVersion.model_id` unset

```bash
$ python -m ml.promote --show
fraud-detector @production -> 1
  artifact location: gs://aide-playground-lakehouse/mlflow/1/models/m-d56b…/artifacts
```

### Promotion is manual, on purpose

CLAUDE.md leaves promotion as an open decision and says to build the manual path
first, so training **always registers** and never promotes. `ml/promote.py` is the
separate human act, and it is idempotent.

The automated alternative — a watcher promoting any version that beats the
incumbent on validation PR-AUC — is not built, and the reason is not effort.
Without ground truth at serving time, "better on validation" is not evidence a
model is better in production; that inference needs the proxy metrics §13
introduces (prediction-distribution drift, disagreement rate, fraud-flag rate).
Auto-promotion belongs after those exist, not before.

## The Kubeflow pipeline (§5)

[`ml/pipeline.py`](../ml/pipeline.py), compiled to
[`ml/pipeline.yaml`](../ml/pipeline.yaml) (KFP SDK 2.17, schema 2.1.0):

```text
train-and-register  ->  quality-gate
```

Both steps run the **same `training:` image** the Kubernetes Job runs, as
`container_component`s rather than `@dsl.component` functions. A function
component would have KFP pip-install dependencies at run time, so a pipeline run
could resolve a different feast or scikit-learn than the model was tested
against; sharing one image makes a pipeline run and a manual run the same code by
construction.

Steps hand off through a run-scoped GCS URI rather than KFP artifacts, which keeps
each step independently runnable with plain `kubectl` — useful precisely when a
pipeline run is the thing that is broken.

The gate enforces **lift over the base rate**, not an absolute PR-AUC. Validation
prevalence moves with the generator's drift window, so a fixed threshold silently
tightens or loosens as the data shifts; "at least 2× better than random" means the
same thing in every window.

### What is verified, and what is not

| | State |
|---|---|
| Pipeline compiles | yes — 2 tasks, correct dependency edge, correct image |
| Both steps run correctly | yes — as Kubernetes Jobs, and as a KFP run |
| Gate blocks a bad model | yes — exit 0 at `min_lift=2.0` (3.25× actual), exit 1 at `min_lift=99` |
| Submitted to a KFP control plane | yes — KFP 2.17 in `ml-ns`, run `Succeeded`, registered `fraud-detector` v5 and passed the gate at 3.25× |
| Run visible in the Kubeflow UI | yes — `kubectl port-forward -n ml-ns svc/ml-pipeline-ui 3000:80` |

### Deploying the control plane

[`charts/kubeflow/`](../charts/kubeflow) is a kustomize overlay on upstream's
`platform-agnostic` env at 2.17.0 — 12 Deployments in `ml-ns`. It carries seven
patch groups, and the split between them is the useful part: three are choices
this project made, four are upstream defects. Each is argued in
`charts/kubeflow/kustomization.yaml`; the summary:

| Patch | Why |
|---|---|
| Generated secrets + MySQL root password | upstream ships `root` with *no* password and `minio`/`minio123` in committed manifests; `bootstrap-secrets.sh` generates both instead (§14) |
| Resource requests, PVC sizes | Autopilot bills pod requests, and 7 containers ship with none — 3.5 vCPU and 14 GiB of billed idle, plus 40 GiB of disk that outlives a parked session |
| `kubeflow-pipelines-public` RoleBinding narrowed | Autopilot's Warden rejects binding a Role to Group `system:authenticated`, and correctly |
| Remove `cache-server` / `cache-deployer` | the deployer mints its TLS cert via a CSR with `O=system:nodes`; Autopilot's `autogke-csr-limitation` forbids node impersonation. Both are v1 components — v2 caching lives in the driver, so nothing is lost |
| `OBJECTSTORECONFIG_HOST`, launcher `providers`, `metadata-writer` POD_NAMESPACE, Argo `artifactRepository` | four separate places where upstream hardcodes the string `kubeflow` as a namespace, in env values and ConfigMap bodies that kustomize's namespace transformer cannot reach |

**MySQL, not Postgres — a deliberate deviation from CLAUDE.md.** The
`platform-agnostic-postgresql` variant was tried first, because it matches the
locked decision. Four manifest-level defects in, the API server reached
`column "defaultexperimentid" does not exist (SQLSTATE 42703)`: KFP builds queries
with squirrel using bare mixed-case identifiers while GORM creates those columns
quoted. MySQL is case-insensitive and never noticed; Postgres folds the unquoted
form to lower case. That is compiled into the binary, so no overlay reaches it.

The deviation is narrower than it looks. The locked decision is about *our*
metadata — Airflow's and MLflow's, still on `data-ns/postgres`. This database
holds KFP's own run bookkeeping: control-plane state, regenerable, dead with the
cluster. The same argument covers SeaweedFS (KFP's internal artifact store)
against "GCS replaces MinIO" — Bronze/Silver/Gold are on GCS; this holds compiled
pipeline packages.

**Step pods run as `pipeline-runner`, not `training`.** That is a different KSA
than the equivalent Kubernetes Job uses, so it needs its own Workload Identity
binding — added to `terraform.tfvars` and annotated by `bootstrap-secrets.sh`.
Without it the training step fails at the first Feast read with a 403 while the
identical code succeeds as a Job.

**No distributed training step.** §5 asks for one. The model is a logistic
regression on 2,118 rows with nothing to distribute, and the gradient-boosted
model that would have justified it was removed deliberately. This is a known gap,
not an oversight.

## What this hands to the next steps

| Next | Takes from here |
|---|---|
| §5 remainder | the control plane is deployed and a run has gone green; what is still missing is a distributed training step, which needs a model that justifies one |
| §12 retraining trigger | the drift DAG's last step calls `ml/submit.py`'s in-cluster path — `--host http://ml-pipeline.ml-ns.svc.cluster.local:8888` |
| §1 `fraud-prediction-api` | `ml/config.py:MODEL_FEATURES` is the exact Redis read list; `aliased_model_location()` gives the model path; the logged signature is the request contract |
| §8 KServe | resolve `@production` to a `gs://` path and point an `InferenceService` at it |
| §9 validation | the four repositories are the seams to fake; the notebook's reload-and-compare check becomes the `hypothesis` idempotency test |
| §12 / §13 | validation PR-AUC and fraud-flag rate are the baseline a challenger is measured against; a second alias is the champion/challenger split |

Known limits: the positive class is small enough that metrics have wide error bars
(rolling-window cross-validation belongs in the pipeline, where it can be run
repeatably), and there are no streaming features yet — `feat_stream_30m` needs
Kafka/Flink on GKE, and needs Job 1 writing to the offline store first or the
feature is untrainable.
