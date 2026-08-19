# Feature Store (Feast)

Serves features to two consumers with different needs: the training pipeline reads
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
      └──────────────► gs://…/delta/gold/<table>/   Delta  — versioned training snapshots
                                │
                    feast apply │ (registry in GCS)
                                ▼
                 feast materialize-incremental
                                ▼
                    Redis  (online store)
                                ▼
                     fraud-prediction-api
```

The export writes the same rows twice on purpose. Feast reads the Parquet copy;
the training pipeline reads the Delta copy, where the transaction log versions
every write for free, so a run can pin `versionAsOf` and log the number to MLflow
(§7). Keeping Delta parallel rather than substituting it means there is no
"does Feast read Delta" question to answer.

Gold is not read directly by Feast because the community ClickHouse offline store
is unstable — CLAUDE.md routes through GCS Parquet instead.

## Entities and feature views

The problem has two grains, so there are two of each.

| View | Entity | Features | Source |
|---|---|---|---|
| `customer_90d` | `customer_id` | 5 trailing-90-day aggregates | `feat_customer_90d` |
| `claim_features` | `claim_id` | 13 claim/policy/customer attributes | `obt_claims_enriched` |

A prediction joins both: `claim_features` describes the event being scored,
`customer_90d` describes the history it sits in.

### TTL rationale

TTL is not a cleanup setting. It is how far back Feast will reach for a value, so
it changes both what the online store serves and which rows survive a training
join. Getting it wrong shrinks the training set or silently serves nothing.

**`customer_90d` — 120 days**, against a 90-day aggregation window. The TTL has
to cover the window plus however late a refresh might be. The batch DAG rebuilds
these daily, so 120 days leaves roughly a month of slack: a few missed runs
degrade freshness instead of causing outright online misses and dropping rows from
historical joins.

**`claim_features` — 3650 days**, effectively none. These are immutable facts
about an event that already happened; a claim's amount does not become stale. A
short TTL here would quietly drop older claims out of training joins and shrink
the dataset for no reason. The long value is the honest expression of "never
expires".

**A streaming view would invert this.** `feat_stream_30m` would take minutes, not
months, because a stale near-real-time signal is worse than no signal — a model
told "30-minute activity" about a value from yesterday is being actively misled.
That view arrives with the Kafka/Flink work.

### One feature deliberately excluded

`claim_status` is in `obt_claims_enriched` and is **not** in the feature view. It
records the outcome of claim processing, which is decided after any fraud
assessment. Including it would be target leakage: strong validation scores, and at
real prediction time the value is either unavailable or already contaminated by
the answer.

## The three pipelines

CLAUDE.md §3 defines three separately deployed jobs. Only the first exists today.

| Pipeline | Direction | Status |
|---|---|---|
| **Materialize Pipeline** | Gold → GCS Parquet → Redis | Built — [`dags/feature_store_materialize.py`](../dags/feature_store_materialize.py) |
| **Job 1** | Flink streaming features → offline store | **Blocked** — no streaming source |
| **Job 2** | Flink streaming features → Redis (push API) | **Blocked** — no streaming source |

Jobs 1 and 2 consume Flink's output, and the Kafka/Flink work is deferred, so
there is nothing for them to read. They are not stubbed, because a stub would
imply the streaming feature path is exercised when it is not.

Worth recording why both writes exist, since it is the non-obvious part of the
design: Job 2 keeps Redis fresh for serving, while Job 1 is what makes a streaming
feature *trainable*. Redis entries expire, so without an offline record the
feature could never be joined against labels — and a model using it in production
would be scoring on a signal it never saw during training. That is train/serve
skew, and it is silent.

## Two images, not one

`feast[redis,gcp]` resolves to numpy 2.2 and pandas 2.3. PySpark 3.5.1 does not
work with numpy 2.x, so installing Feast into the Airflow/Spark image would break
every Spark job's pandas conversion — including the export that feeds Feast.

So the Materialize Pipeline spans two images: the export task runs in the Airflow
image (Spark, Delta, GCS connector), and the Feast tasks run as
`KubernetesPodOperator` against [`dockerfile.feast`](../dockerfile.feast). This
also matches §8, which wants a separate pipeline per job.

## Verification

The DAG's last task is a real online read, not a trust of `materialize`'s exit
code. Materialization can report success while online reads return nothing —
wrong entity-key serialization, a TTL that excludes every row, Redis in the wrong
namespace. Each looks identical to a healthy store until predictions start coming
back empty, so
[`feature_store/verify_online.py`](../feature_store/verify_online.py) reads known
entity keys back out and exits non-zero if any feature is entirely null.

```bash
# Manually, against the deployed store
kubectl run feast-verify -n data-ns --rm -it --restart=Never \
  --image=northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/feast:0.1.0 \
  --overrides='{"spec":{"serviceAccountName":"airflow"}}' \
  --command -- sh -c "cd /feature_store && python verify_online.py"
```

## Redis configuration notes

Redis runs in `api-serving-ns`, not `data-ns`, because `fraud-prediction-api`
reads it synchronously on the prediction path. That means every client outside
that namespace must use the fully-qualified name — a bare `redis:6379` resolves
only within the pod's own namespace and fails from Airflow.

AOF persistence is on. The data is regenerable by re-materializing, but without
persistence a pod restart empties the store silently and the API returns "no
features" for every request — a failure that reads like an application bug.

No password yet. The only clients are in-cluster, and inventing one now would put
a plaintext secret in `feature_store.yaml`, which §14 rules out. Real credentials
arrive with Vault.
