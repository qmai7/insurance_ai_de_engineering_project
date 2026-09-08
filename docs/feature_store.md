# Feature Store (Feast)

Feast or feature store exist to make sure the features a model sees at training time and 
the features it sees at prediction time are computed the same way.
Serves features to two consumers(offline and online store) with different needs: the training pipeline reads
history and needs point-in-time correctness; the prediction API reads one entity
and needs it in milliseconds. Feast keeps one definition for both.

- **Registry** — `gs://aide-playground-lakehouse/feast/registry.db`
- **Offline store** — Parquet on GCS, exported from Gold
- **Online store** — Redis (`redis.api-serving-ns.svc.cluster.local:6379`)
- **Definitions** — [`feature_store/features.py`](../feature_store/features.py)

## Data flow

```text
ClickHouse Gold
      │  jobs/export_gold_to_feast.py
      ├──────────────► gs://…/feast/<table>/        Parquet — Feast offline source
      └──────────────► gs://…/delta/gold/<table>/   Delta  — versioned training snapshots since Feast's FileSource can only read Parquet files.
                                │
                    feast apply │ (registry in GCS)
                                ▼
                 feast materialize-incremental
                                ▼
                    Redis  (online store)
                                ▼
                     fraud-prediction-api
```

## Entities and feature views

The problem has two grains, so there are two of each.

| View | Entity | Features | Source | Source grain |
|---|---|---|---|---|
| `customer_90d` | `customer_id` | 5 trailing-90-day aggregates | `feat_customer_90d` | one row per (customer, as-of date) |
| `claim_features` | `claim_id` | 14 claim/policy/customer attributes | `obt_claims_enriched` | one row per claim |

A prediction joins both: `claim_features` describes the event being scored,
`customer_90d` describes the history it sits in.

`feat_customer_90d` being a **time series rather than a snapshot** is the part that
is easy to get wrong — it was wrong here first. See
[the traps section](#a-snapshot-of-aggregates-cannot-be-joined-to-history) below.

### TTL rationale

TTL is not a cleanup setting. It is how far back Feast will reach for a value, so
it changes both what the online store serves and which rows survive a training
join. Getting it wrong shrinks the training set or silently serves nothing.

**`customer_90d` — 120 days**, against a 90-day aggregation window. The TTL has
to cover the window plus however late a refresh might be. The batch DAG rebuilds
these daily, so 120 days leaves roughly a month of slack: a few missed runs
degrade freshness instead of causing outright online misses and dropping rows from
historical joins.

Note what the TTL actually does offline now that the source is a time series: a
claim joins to the feature row stamped with its own filing date, so the TTL only
rejects a claim whose *nearest* feature row is more than 120 days older. Against a
snapshot source, the same 120 days rejected almost every claim in the dataset — the
setting was identical, the outcome was not.

**`claim_features` — 3650 days**. These are immutable facts
about an event that already happened; a claim's amount does not become stale. A
short TTL here would quietly drop older claims out of training joins and shrink
the dataset for no reason. The long value is the expression of "never
expires".

### One feature deliberately excluded, one deliberately added

`claim_status` is in `obt_claims_enriched` and is **not** in the feature view. Thus, it won't be used to train our fraud prediction model. It
records the outcome of claim processing (approved or denied), which is decided after any fraud
assessment. 

`days_since_policy_start` is the opposite case, and was added when the model went
looking for signal it could not see. A claim filed days into a new policy is a
classic fraud indicator, and it is one of the drivers the generator actually uses
(§6) — but `obt_claims_enriched` only carried `policy_start_date` and `claim_date`
separately, so the view exposed the raw dates and not the interval between them.
Fraud rate is **7.5% for claims filed within 30 days of policy start against 2.4%
after**, so leaving it out cost roughly a 3× signal for nothing. It is known the
instant the claim arrives, so it is a feature and not leakage.

## The three pipelines

CLAUDE.md §3 defines three separately deployed jobs. Only the first exists today.

| Pipeline | Direction | Status |
|---|---|---|
| **Materialize Pipeline** | Gold → GCS Parquet → Redis | Built — [`dags/feature_store_materialize.py`](../dags/feature_store_materialize.py) |
| **Job 1** | Flink streaming features → offline store | **Hold-off**  |
| **Job 2** | Flink streaming features → Redis (push API) | **Hold-off** |

Jobs 1 and 2 consume Flink's output, and the Kafka/Flink work is deferred, so
there is nothing for them to read. They are not stubbed, because a stub would
imply the streaming feature path is exercised when it is not.

Worth recording why both writes exist, since it is the non-obvious part of the
design: Job 2 keeps Redis fresh for serving, while Job 1 is what makes a streaming
feature *trainable*. Redis entries expire, so without an offline record the
feature could never be joined against labels — and a model using it in production
would be scoring on a signal it never saw during training. That is train/serve
skew, and it is silent.

## Verification

The DAG's last task is a real online read, not a trust of `materialize`'s exit
code. Materialization can report success while online reads return nothing

[`feature_store/verify_online.py`](../feature_store/verify_online.py) reads entity
keys back out and compares them, feature by feature, against the offline source
they were materialized from. Two properties are worth more than the check itself:

```bash
# Manually, against the deployed store
kubectl run feast-verify -n data-ns --rm -it --restart=Never \
  --image=northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/feast:0.1.5 \
  --overrides='{"spec":{"serviceAccountName":"airflow"}}' \
  --command -- sh -c "cd /feature_store && python verify_online.py"
```

## Feast UI

The installed Feast CLI includes a local UI for browsing the feature repository's
metadata: entities, feature views, features, data sources and registry details.


From the repository root, start it with the ML virtual environment:

```bash
# Required once in .venv-ml for Feast 0.65 UI dependencies:
.venv-ml/bin/python -m pip install \
  "grpcio-health-checking==1.76.0" \
  "grpcio-reflection==1.76.0"

.venv-ml/bin/feast -c feature_store ui \
  --host 127.0.0.1 \
  --port 8888
```

Open [http://127.0.0.1:8888](http://127.0.0.1:8888) in a browser.

The UI reads the local `feature_store/feature_store.yaml` and the registry path
configured there. To inspect actual online values, use Feast's SDK or
[`feature_store/verify_online.py`](../feature_store/verify_online.py); raw Redis
keys are serialized and are not intended to be read through this UI.


## Redis (online store) configuration notes

Redis runs in `api-serving-ns`, not `data-ns`, because `fraud-prediction-api`
reads it synchronously on the prediction path. That means every client outside
that namespace must use the fully-qualified name — a bare `redis:6379` resolves
only within the pod's own namespace and fails from Airflow.
