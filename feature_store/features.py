"""
Feast feature definitions for insurance fraud detection.

Two entities, because the problem has two natural grains:

  claim     the model's grain — one prediction per claim
  customer  the aggregate grain — behavioural history behind that claim

A prediction joins both: claim attributes describe the event, customer aggregates
describe the history it sits in.

Sources are the Parquet exports written by jobs/export_gold_to_feast.py, not
ClickHouse directly, because Feast's community ClickHouse offline store is
unstable (CLAUDE.md).
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Bool, Float64, Int64, String

LAKEHOUSE = "gs://aide-playground-lakehouse"

# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

claim = Entity(
    name="claim",
    join_keys=["claim_id"],
    description="A single insurance claim. The grain the fraud model predicts on.",
)

customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="A policyholder. Carries aggregate behavioural features.",
)

# ---------------------------------------------------------------------------
# Sources
#
# created_timestamp_column matters for correctness, not bookkeeping: when two rows
# share an event_timestamp, Feast uses it to decide which one wins. Without it,
# ties resolve arbitrarily.
# ---------------------------------------------------------------------------

customer_90d_source = FileSource(
    name="feat_customer_90d_source",
    path=f"{LAKEHOUSE}/feast/feat_customer_90d",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    description="Trailing-90-day customer aggregates exported from Gold.",
)

claim_source = FileSource(
    name="obt_claims_enriched_source",
    path=f"{LAKEHOUSE}/feast/obt_claims_enriched",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    description="Claim-level attributes joined to policy and customer context.",
)

# ---------------------------------------------------------------------------
# Feature views
# ---------------------------------------------------------------------------

customer_90d_fv = FeatureView(
    name="customer_90d",
    entities=[customer],
    # TTL 120 days, against a 90-day aggregation window.
    #
    # TTL is how far back Feast will reach for a value, so it has to exceed the
    # window plus however long a refresh might be late. The batch DAG rebuilds
    # these daily; 120 days leaves a month of slack, so a few missed runs degrade
    # freshness instead of causing outright lookup misses online and silently
    # dropping rows from training joins.
    #
    # A streaming view such as feat_stream_30m would take the opposite setting —
    # minutes, not months — because a stale near-real-time feature is worse than
    # no feature. That view arrives with the Kafka/Flink work.
    ttl=timedelta(days=120),
    online=True,
    source=customer_90d_source,
    schema=[
        Field(name="f_customer_avg_claim_amount_90d", dtype=Float64),
        Field(name="f_customer_total_claims_90d", dtype=Int64),
        Field(name="f_customer_total_claim_amount_90d", dtype=Float64),
        Field(name="f_customer_total_payments_90d", dtype=Float64),
        Field(name="f_customer_payment_failure_rate_90d", dtype=Float64),
    ],
    description="Customer behaviour over the trailing 90 days.",
    tags={"grain": "customer", "refresh": "daily-batch"},
)

claim_features_fv = FeatureView(
    name="claim_features",
    entities=[claim],
    # TTL 10 years, i.e. effectively none.
    #
    # These are immutable facts about an event that already happened — a claim's
    # amount does not go stale. A short TTL here would quietly drop older claims
    # out of training joins, shrinking the training set for no reason. Long TTL is
    # the correct expression of "this never expires".
    ttl=timedelta(days=3650),
    online=True,
    source=claim_source,
    schema=[
        Field(name="claim_amount", dtype=Float64),
        Field(name="premium_amount", dtype=Float64),
        Field(name="claim_to_premium_ratio", dtype=Float64),
        Field(name="claim_type", dtype=String),
        Field(name="policy_type", dtype=String),
        Field(name="policy_status", dtype=String),
        Field(name="province", dtype=String),
        Field(name="risk_segment", dtype=String),
        Field(name="age", dtype=Int64),
        Field(name="marketing_opt_in", dtype=Bool),
        Field(name="claim_month", dtype=Int64),
        Field(name="claim_day_of_week", dtype=Int64),
        Field(name="claim_is_weekend", dtype=Bool),
        # claim_status is deliberately excluded.
        #
        # It records the outcome of claim processing, which is decided after any
        # fraud assessment. Feeding it to the model would be target leakage: it
        # would score well in validation and be unavailable — or misleading — at
        # the moment a real prediction is needed.
    ],
    description="Attributes of the claim being scored, plus policy/customer context.",
    tags={"grain": "claim", "refresh": "daily-batch"},
)
