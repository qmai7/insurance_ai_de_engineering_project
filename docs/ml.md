# ML — baseline fraud model

The notebook is
[`notebooks/01_fraud_model_baseline.ipynb`](../notebooks/01_fraud_model_baseline.ipynb);
environment setup is in [`notebooks/README.md`](../notebooks/README.md). This page
records the decisions and the numbers, so neither has to be reverse-engineered from
cell outputs.

Big picture:

The notebook proved the model logic.
The Kubeflow pipeline is the repeatable automation of the same logic.
The pipeline does not invent a new model. It runs the same training path in a graph, then logs and registers the result in MLflow, and finally applies a quality gate before it is considered worth promoting.

What does the Kubeflow pipeline do? 

- load the right features from Feast
- join them to labels from Bronze
- split on time, not randomly
- train a fraud model
- score it on validation
- log everything to MLflow
- register the model
- decide whether it passes the quality threshold

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
                          LogisticRegression (model training)
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

### One model

Logistic regression only. A boosted model was tried and scored within noise of it
(PR-AUC 0.329 vs 0.358) on a 582-row validation set with 64 positives (11%), which is not
enough evidence to prefer either — so it was removed rather than kept as decoration.


## Results

This model returns a fraud risk score per claim, not a single number for the whole dataset. That means for each row/ claim, it outputs a probability between 0 and 1, in which 0 is very unlikely fraud.

```python
model.predict_proba(sample_row)
# array([0.96, 0.04])
```
First column = probability of "not fraud" 
2nd column = probability of "fraud"

The pipeline then uses that 2nd column as the fraud-risk score for ranking
```python
prob_fraud = model.predict_proba(X_val)[:, 1]
# array([0.01, 0.02, 0.03, ..., 0.90, 0.91, 0.92])
```

Then review decision:
```python
threshold = np.quantile(prob_fraud, 1 - budget)
flagged = prob_fraud >= threshold
```
Why the review decsion is done as above: 
- the business question is “which claims should my team review first, given limited reviewer capacity?”
- the business question is not “is it fraud?” in the abstract

Example: 
if review budget = 10% = 0.1 => threshold = 90th percentile of the fraud score.
Suppose 1,000 claims have fraud probabilites like: 0.01, 0.02, 0.03, ..., 0.90, 0.91, 0.92. Then, 90th percentile is around 0.82. Thus, all claims with probability **>= 0.82** are flagged and about 100 claims are reviewed


**PR-AUC 0.358 · ROC-AUC 0.751**

The operating point is a **review budget**. Nobody
investigates every claim, so the question is what a team reviewing the top N% by
risk actually sees.

| Review budget | Flagged | Fraud caught | Precision | Recall | Lift vs random |
|---|---|---|---|---|---|
| 5% | 30 | 16 / 64 | 53.3% | 25.0% | 4.9× |
| **10%** | **59** | **21 / 64** | **35.6%** | **32.8%** | **3.2×** |
| 20% | 117 | 31 / 64 | 26.5% | 48.4% | 2.4× |
| 30% | 175 | 39 / 64 | 22.3% | 60.9% | 2.0× |

The model supplies the ordering; the budget decides where to cut.

### Reading these numbers correctly
PR-AUC (Precision-Recall Area Under the Curve):
This is the headline metric for rare positive events.

What it measures:

- How well the model ranks claims from most likely fraud to least likely fraud
- It rewards putting real fraud high in the list
- It ignores “how many legit claims were also ranked low” as much as the positive-class focus requires

For a fraud model:

- the positive class is “fraud”
- with only ~11% fraud, PR-AUC is far more informative than ROC-AUC or accuracy

So the code is doing: 
- “take the fraud probability for each validation claim”
- “ask sklearn to compute the area under the precision-recall curve”
- “that value is the PR-AUC”

* Precision at a budget is the number that matters: at 10%, roughly **1 flagged claim
in 3 is fraud, against 1 in 9 for random review**. The same investigator hours find
about three times as much fraud. Tighten to a 5% budget and it is better than a
coin flip.



## From notebook to production path

The notebook is the exploration. `ml/` is the same model as deployable code. The two agree: the training job reproduces the
notebook's metrics exactly (PR-AUC 0.3577, ROC-AUC 0.7508), which is the only
useful proof that the productionised path did not quietly change the model.

```text
ml/config.py        Central configuration and feature definitions
ml/repositories.py  Reads data from GCS, Parquet, Feast, and Delta
ml/services.py      Contains the actual training, evaluation, and MLflow logic
ml/train.py         Orchestrates one complete training run
ml/gate.py          Checks whether the trained model meets the quality threshold
ml/promote.py       Manually points MLflow production to a model version
ml/pipeline.py      the Kubeflow pipeline definition
```

### config.py - configuration file:
It answers: “what is the path, what are the columns, what is the model config?”
It defines Lakehouse root, Feast repo path, label path, Delta table path, entity keys, etc... 
It should be the same across notebook, local training, Kubeflow, tests

### repositories.py - data access
It answers: “how do the system fetch this data from the actual source?”
It hides Feast, GCS, Parquet, Delta details
It lets `ml/services.py` stay logic-focused

### services.py - the model logic :
are represented by the 5 following classes:
| Service | Responsibility |
|---|---|
| `TrainingDataService` | build the final training dataset; assert the join is intact |
| `SplitService` | time-based  split for training and validation dataset |
| `ModelBuilder` | builds the LogisticRegression pipeline with preprocessing |
| `EvaluationService` | computes PR-AUC, ROC-AUC, precision/recall by review budget |
| `ModelRegistryService` | logs everything to MLflow and registers the model |

The classes above are meant to be reused by: notebook, a kubeflow component 

### train.py - orchestrator:

- it wires the **repositories** and **services** together
- reads config from ml/config.py
- loads the Feast data / limits / tracking URI / the model name
- executes the flow in order
- writes summary JSON for the next pipeline step
- calls MLflow logging

### gate.py - pipeline gate 

It reads the JSON summary written by the training step(train.py) and checks:

1. PR-AUC (Precision-Recall Area Under the Curve)
2. base rate = fraud rate (64 fraud / 582 claims = 11%)
3. lift over baseline
This is a relative metric. It tells you how much better the model is than random ordering.

```python
lift = pr_auc / baseline if baseline else 0.0
```
Example:

- baseline = 0.11
- pr_auc = 0.358
Then:

``` text
0.358 / 0.11 = 3.25
```
So the model is about 3.25x better than random at ranking fraud to the top.

Decision making: 
- if the model’s PR-AUC is weak relative to the base rate => the model is not above random => the gate fails

### gate.py - The quality gate:
The gate asks:

- is the model’s PR-AUC high enough relative to baseline?
- do we meet the minimum lift threshold?
It is not a “promotion” step by itself. Promotion is still manual. The gate decides if a version is eligible, and then the human runs promotion.

#### Promotion is manual, on purpose

We leave promotion as an open decision, so training **always registers** and never promotes. `ml/promote.py` is the model promotion script that can be run manually.

The automated alternative — a watcher promoting any version that beats the
incumbent on validation PR-AUC — is not built, and the reason is not effort.
Without ground truth at serving time, "better on validation" is not evidence a
model is better in production; that inference needs the proxy metrics §13
introduces (prediction-distribution drift, disagreement rate, fraud-flag rate).
Auto-promotion belongs after those exist, not before.

## pipeline.py - The Kubeflow pipeline

[`ml/pipeline.py`](../ml/pipeline.py), compiled to
[`ml/pipeline.yaml`](../ml/pipeline.yaml) - Kubeflow Pipelines (KFP) SDK 2.17, schema 2.1.0

### 6-Step Training Pipeline (Refactored)

The pipeline was refactored from a monolithic 2-step design into 6 granular, independently-debuggable steps:

```text
1. build_training_dataset  ─┐
                            ├─ Load Feast features + labels
                            ↓
2. split_by_time  ──────────┤ Temporal train/validation split (80th percentile)
                            ↓
3. train_model  ────────────┤ Fit logistic regression
                            ↓
4. evaluate_model  ─────────┤ Score validation, compute PR-AUC/ROC-AUC/budget curves
                            ↓
5. log_to_mlflow  ──────────┤ Log run to MLflow, register model version
                            ↓
6. quality_gate  ───────────┤ Check if PR-AUC / baseline_PR-AUC >= min_lift (default 2.0)
                            ↓
                        Pass / Fail
```

Each step:
- **Reads** artifacts written by the previous step (except step 1)
- **Writes** artifacts to GCS for the next step (except step 6)
- **Runs independently** in its own container with isolated resources
- **Can be debugged** by inspecting intermediate artifacts without re-running prior steps

Artifact handoff via GCS paths:
```
gs://aide-playground-lakehouse/mlflow/pipelines/{run_id}/
├── dataset.joblib        Step 1 output → Step 2 input
├── split.joblib          Step 2 output → Steps 3-4 input
├── model.joblib          Step 3 output → Step 4 input
├── evaluation.joblib     Step 4 output → Step 5 input
└── summary.json          Step 5 output → Step 6 input (quality gate)
```

### train.py - Step-wise execution support

[`ml/train.py`](../ml/train.py) now supports two modes:

**All steps in sequence (default):**
```bash
.venv-ml/bin/python -m ml.train --summary-uri gs://.../summary.json
```

**Individual step execution (for debugging or Kubeflow):**
```bash
# Step 1: Build dataset
.venv-ml/bin/python -m ml.train --step build_dataset \
  --dataset-uri gs://.../dataset.joblib

# Step 2: Split by time (reads output of step 1)
.venv-ml/bin/python -m ml.train --step split_by_time \
  --dataset-uri gs://.../dataset.joblib \
  --split-uri gs://.../split.joblib

# Step 3: Train model (reads output of step 2)
.venv-ml/bin/python -m ml.train --step train \
  --split-uri gs://.../split.joblib \
  --model-uri gs://.../model.joblib

# Step 4: Evaluate (reads outputs of steps 2-3)
.venv-ml/bin/python -m ml.train --step evaluate \
  --split-uri gs://.../split.joblib \
  --model-uri gs://.../model.joblib \
  --evaluation-uri gs://.../evaluation.joblib

# Step 5: Log to MLflow (reads all intermediate artifacts)
.venv-ml/bin/python -m ml.train --step log_to_mlflow \
  --dataset-uri gs://.../dataset.joblib \
  --split-uri gs://.../split.joblib \
  --model-uri gs://.../model.joblib \
  --evaluation-uri gs://.../evaluation.joblib \
  --summary-uri gs://.../summary.json
```

Each step can be **restarted independently** — e.g., if step 4 (evaluate) fails due to a transient error, rerun only step 4 without re-running steps 1-3. This saves ~30 minutes of Feast I/O and training.

### Resource allocation by step

| Step | CPU | Memory | Duration | Purpose |
|---|---|---|---|---|
| build_training_dataset | 1-2 | 3-4Gi | ~15m | Feast I/O + join |
| split_by_time | 1-2 | 3-4Gi | <1m | DataFrame operations |
| train_model | 1-2 | 3-4Gi | ~5m | Model fitting |
| evaluate_model | 500m-1 | 2Gi | <1m | Scoring |
| log_to_mlflow | 500m-1 | 2Gi | <1m | MLflow API calls |
| quality_gate | 200m | 512Mi | <1m | Threshold check |

### Benefits of granular steps

1. **Failure Isolation:** Know exactly which step failed and why
2. **Faster Debugging:** Rerun only the failed step, not the entire pipeline
3. **Transparent Monitoring:** See resource usage and duration per step in Kubeflow UI
4. **Independent Testing:** Unit-test each step function in isolation
5. **Partial Reruns:** If upstream succeeds, start from the failure point

## submit.py - Compiles and submits the pipeline to Kubeflow

[`ml/submit.py`](../ml/submit.py):
-> compiles `pipeline.py` to `pipeline.yaml` (6 components)
-> submits to Kubeflow API
-> trains container runs `ml.train --step <name>` for steps 1-5
-> gate container runs `ml.gate` for step 6


## How to run

```bash
python -m ml.submit --wait
```
or via Kubeflow UI 

Then, after reviewing the result:

![Kubeflow_pipeline](/assets/kube_flow.png)


Data Scientist can perform the following to promote a model
```bash
.venv-ml/bin/python -m ml.promote --show
.venv-ml/bin/python -m ml.promote --version <version>
```


### Check Kubeflow 
1. Check whether the KFP services exist
```bash
kubectl get pods -n ml-ns
kubectl get svc -n ml-ns | grep ml-pipeline
```

2. Expose the UI locally. If the UI service is in ml-ns, use:
```bash
kubectl -n ml-ns port-forward svc/ml-pipeline-ui 8080:80
```

then open: http://localhost:8080

3. Check the model information logged in MLFlow at http://localhost:5000 (in ml-ns)
if not reachable, run: 
```bash
kubectl -n ml-ns port-forward svc/mlflow 5000:5000
```

![ml_flow_model_registry](/assets/ml_flow_model_registry.png)

A model run history

![ml_flow_training_runs](/assets/ml_flow_training_runs.png)

Example of a run detail

![ml_flow_run_info](/assets/ml_flow_run_info.png)


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
