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

The offline store has a second reader that is easy to forget: training. The
notebook and the §5 pipeline call `get_historical_features` against the same Parquet
that feeds `materialize`, which is what makes the offline grain a *training*
concern and not just a storage detail — see [`ml.md`](ml.md).

The export writes the same rows twice on purpose. Feast reads the Parquet copy;
the training pipeline reads the Delta copy, where the transaction log versions
every write for free, so a run can pin `versionAsOf` and log the number to MLflow
(§7). 

Gold is not read directly by Feast because the community ClickHouse offline store
is unstable.

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

**A streaming view would invert this.** `feat_stream_30m` would take minutes, not
months, because a stale near-real-time signal is worse than no signal — a model
told "30-minute activity" about a value from yesterday is being actively misled.
That view arrives with the Kafka/Flink work.

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
code. Materialization can report success while online reads return nothing — wrong
entity-key serialization, a TTL that excludes every row, Redis in the wrong
namespace, a write the online store declined. Each looks identical to a healthy
store until predictions start coming back empty.

[`feature_store/verify_online.py`](../feature_store/verify_online.py) reads entity
keys back out and compares them, feature by feature, against the offline source
they were materialized from. Two properties are worth more than the check itself:

- **The feature list comes from the registry, not the script.** A feature added to
  a view is verified on the next run without anyone remembering to add it. The
  hardcoded version is how the `days_since_policy_start` bug above stayed invisible.
- **It separates `MISSING` from `STALE` from `ABSENT`.** Same symptom — an empty
  prediction — three different causes, and knowing which one saves the investigation.

Comparing against the source also catches the case a null-check cannot: Redis
persists across runs, so a materialization that loads nothing at all leaves the
previous run's values in place and every feature looks populated.

```bash
# Manually, against the deployed store
kubectl run feast-verify -n data-ns --rm -it --restart=Never \
  --image=northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/feast:0.1.5 \
  --overrides='{"spec":{"serviceAccountName":"airflow"}}' \
  --command -- sh -c "cd /feature_store && python verify_online.py"
```

## Four traps worth knowing

All of these leave a feature store that reports success and is quietly wrong. The
first breaks training, the rest break serving; none of them raises an error. They
are the reason verification compares values against the offline source rather than
checking for nulls, and the reason the training notebook asserts on row count.

### A snapshot of aggregates cannot be joined to history

`feat_customer_90d` was originally one row per customer, every row stamped with the
newest claim date in the dataset. Online serving worked perfectly — one current
value per customer is exactly what Redis needs.

Training got **zero rows**.

A point-in-time join takes the newest feature row *at or before* the entity's
timestamp. Asked for a claim filed in May, the only customer row available was
stamped August, so Feast filtered it as future data — and dropped the entity row
along with it. Not nulls in a column: no rows at all. Every value in the table was
correct.

The table is now a time series at `(customer_id, as_of_date)`, with as-of dates from
two sources:

- **every date a customer filed a claim** — the training spine, so each claim joins
  to the history that existed when it was filed
- **the newest claim date, for every customer** — the serving row, including
  customers who have never claimed, since an online lookup can arrive for anyone.
  It is also the newest row per customer, which is what `materialize-incremental`
  loads into Redis, so the online store is unchanged by this.

10,000 rows became 12,528. The windows also now end *strictly before* `as_of_date`,
which closed a leak worth more than the row count: the old snapshot's aggregates
counted every claim in the dataset, including the one a model would be scoring.

Guarded by a Gold quality gate on `(customer_id, as_of_date)` uniqueness — a
duplicate pair would make Feast's tie-break arbitrary, because `created_timestamp`
is identical for every row in an export — and by row-count assertions in
[`notebooks/01_fraud_model_baseline.ipynb`](../notebooks/01_fraud_model_baseline.ipynb).

### `as_of_date` must come from the data, not a literal

`feat_customer_90d.as_of_date` becomes Feast's `event_timestamp`, so it decides
whether a row falls inside the view's TTL. It was originally the literal
`2025-11-01` in `gold_clickhouse.py`. Once wall-clock passed the 120-day TTL,
`materialize-incremental` loaded **zero** customer rows and reported success —
Redis held only the 2,700 claim keys and looked healthy.

It is now derived from `max(claim_date)`. That is deliberately not
`current_date()`: the aggregates describe activity up to the newest claim, and
stamping them "now" would overstate their freshness.

The generator had the same problem from the other end, with all dates hardcoded to
2025. Both are fixed; either alone would have kept the bug alive.

### Adding a feature to a view does not reach Redis on its own

This one cost the most to find, because every command in the chain reported
success.

`days_since_policy_start` was added to `claim_features`, Gold was rebuilt, the
export wrote it, `feast apply` registered it (the registry listed 14 features), and
`feast materialize-incremental` ran and recorded its interval. Reading it back
online returned `None`. The raw Redis hash held 14 fields — 13 features plus the
timestamp marker — where 14 features needs 15.

**Feast's Redis online store declines a write whose event timestamp is not newer
than the one already stored.** That is correct behaviour in isolation: it stops an
out-of-order backfill from overwriting fresher values. But the event timestamp here
is `claim_date`, which does not change when the pipeline re-runs. So re-exporting
the same claims with a new column produces writes that Feast skips wholesale — and
skipping a write skips the new field with it. The schema in Redis silently stays at
whatever it was the first time those keys were written.

Nothing in the pipeline noticed, because the old features were all still correct.
Fixing it means clearing the keys, not re-running harder:

```bash
kubectl exec -n api-serving-ns redis-0 -- redis-cli FLUSHDB
gcloud storage rm gs://aide-playground-lakehouse/feast/registry.db  # see below
# then re-run the DAG: apply recreates the registry, incremental starts from end - TTL
```

`verify_online.py` now reads its feature list **from the registry** instead of a
hardcoded list, so any feature added to a view is checked on the next run and this
failure surfaces as a red task rather than a null in a prediction. It also
distinguishes the three ways an online read can be wrong — `MISSING` (offline has a
value, online is null), `STALE` (they disagree) and `ABSENT` (no online row at all)
— because they have different causes and the same symptom.

The general rule: **a feature-view schema change is not idempotent with respect to
the online store.** Batch pipelines are usually safe to re-run; this step is only
safe to re-run when the data changed, not when the definition did.

### Flushing Redis does not reset the materialization watermark

The registry records how far materialization has progressed. Clearing Redis
without clearing that watermark leaves Feast convinced the online store is current,
so `materialize-incremental` loads nothing and exits 0 — with an empty store.

If you flush Redis, reset the watermark too:

```bash
gcloud storage rm gs://aide-playground-lakehouse/feast/registry.db
# the DAG's feast_apply step recreates it; incremental then starts from end - TTL
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
