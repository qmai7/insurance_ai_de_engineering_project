"""
Insurance Data + AI Project - Part 01 Data Generator (Small/Fast Version)

Run:
  uv run python insurance_data_generator.py

Outputs strict Parquet for offline data and JSONL for streaming-style events.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class GeneratorConfig:
    # All generator parameters are centralized here so the project can be scaled
    # up/down without changing the generation logic.
    # Batch/offline table sizes. These are small enough for local Docker runs.
    n_customers: int = 10_000
    n_policies: int = 15_000
    days_history: int = 90

    # Intentional geography skew: most customers are in Quebec.
    qc_ratio: float = 0.70
    ontario_ratio: float = 0.30
    auto_policy_ratio: float = 0.75

    # Intentional data quality problems that Silver must clean.
    duplicate_claim_rate: float = 0.02
    duplicate_stream_rate: float = 0.015

    # Date anchoring.
    #
    # Dates used to be hardcoded to 2025-08-01. That silently breaks the feature
    # store: Feast TTL is measured against wall-clock, so once the fixed dates
    # aged past `customer_90d`'s 120-day TTL, `materialize-incremental` loaded
    # zero customer rows and the online store looked simply empty. Anchoring the
    # window to "now" keeps generated data inside the TTLs by construction.
    
    end_date: str = ""

    # Schema evolution: rows older than this offset have selected columns null.
    # any row whose date falls more than 30 days before end_date, gets risk_segment and payment_method set to null
    schema_change_offset_days: int = 30

    # Fraud labels config (claim_id, is_fraud).
    
    fraud_base_rate: float = 0.06
    fraud_high_ratio_weight: float = 1.30
    fraud_new_policy_weight: float = 0.90
    fraud_high_risk_weight: float = 0.70
    fraud_weekend_weight: float = 0.25

    # Claim types with a higher underlying fraud propensity.
    fraud_prone_claim_types: Tuple[str, ...] = ("theft", "vandalism", "lost_baggage")
    fraud_type_weight: float = 0.80

    # Data drift simulation.

    # Periodic drift detection Airflow DAG pushing metrics via Prometheus Pushgateway
    drift_enabled: bool = True
    drift_window_days: int = 14
    # within that drift window, claim amounts get scaled up ~60% versus the baseline distribution. 
    # E.g. if a normal claim were around $2,000, in the drift window it's generated closer to $3,200.
    drift_claim_amount_multiplier: float = 1.60 
    # the payment failure rate increases by 10 percentage points in that window
    drift_payment_failure_uplift: float = 0.10
    # the odds of a claim being fraudulent doubles in that window
    drift_fraud_odds_multiplier: float = 2.00

    # Streaming controls: normal traffic, burst traffic, late arrivals, and duplicates.
    base_events_per_min: int = 20
    burst_multiplier: int = 10
    burst_windows: Tuple[str, str] = ("08:00-08:20", "20:00-20:20")
    late_arrival_rate: float = 0.12
    late_delay_min_max: Tuple[int, int] = (5, 45)

    random_seed: int = 42
    output_dir: str = "generated_insurance_data"


CONFIG = GeneratorConfig()

QC_CITY_WEIGHTS = {
    "Montreal": 0.45,
    "Quebec City": 0.15,
    "Laval": 0.10,
    "Gatineau": 0.08,
    "Longueuil": 0.07,
    "Sherbrooke": 0.07,
    "Trois-Rivieres": 0.05,
    "Other QC": 0.03,
}

ON_CITY_WEIGHTS = {
    "Toronto": 0.35,
    "Ottawa": 0.20,
    "Mississauga": 0.15,
    "Brampton": 0.08,
    "Hamilton": 0.07,
    "London": 0.07,
    "Kitchener": 0.05,
    "Other ON": 0.03,
}

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "pre_authorized_debit"]
CHANNELS = ["web", "mobile_app", "call_center", "agent"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]


def set_random_seed(seed: int) -> None:
    """Make every run reproducible for grading and debugging."""
    random.seed(seed)
    np.random.seed(seed)


def weighted_choice(weight_dict: Dict[str, float], size: int) -> np.ndarray:
    """Choose values using custom probabilities, used to create realistic skew."""
    keys = list(weight_dict.keys())
    probs = np.array(list(weight_dict.values()), dtype=float)
    probs = probs / probs.sum()
    return np.random.choice(keys, size=size, p=probs)


def random_timestamps(start_date, days: int, size: int) -> pd.Series:
    """Generate random timestamps inside a historical window."""
    start = pd.Timestamp(start_date)
    random_seconds = np.random.randint(0, days * 24 * 60 * 60, size=size)
    return pd.Series(start + pd.to_timedelta(random_seconds, unit="s"))


def date_window(config: GeneratorConfig) -> Dict[str, pd.Timestamp]:
    """
    Resolve the generation window, anchored to `end_date` (default: today UTC).

    Every date in the dataset derives from here rather than a literal, so the data
    stays inside the feature store's TTLs no matter when it is generated. See the
    note on `end_date` in GeneratorConfig for why that matters.
    """
    end = (
        pd.Timestamp(config.end_date)
        if config.end_date
        # Timestamp.utcnow() is deprecated and removed in pandas 4.
        else pd.Timestamp.now("UTC").tz_localize(None).normalize()
    )
    return {
        "end": end,
        "start": end - pd.Timedelta(days=config.days_history),
        "schema_change": end - pd.Timedelta(days=config.schema_change_offset_days),
        "drift_start": end - pd.Timedelta(days=config.drift_window_days),
    }


def sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


_WINDOW: Dict[str, pd.Timestamp] = {}


def window(config: GeneratorConfig) -> Dict[str, pd.Timestamp]:
    """
    The generation window, resolved once per process.

    Cached because the default anchor is "now": recomputing per table could
    straddle midnight and leave tables anchored to different days, which would
    corrupt the schema-evolution and drift cutoffs that depend on them agreeing.
    """
    global _WINDOW
    if not _WINDOW:
        _WINDOW = date_window(config)
    return _WINDOW


# Spark 3.5.1 cannot read nanosecond Parquet timestamps:
#
#   AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
#
# pandas holds datetime64[ns] internally and recent pyarrow writes it faithfully
# as TIMESTAMP(NANOS), so every timestamp column becomes unreadable by the Silver
# job. Coercing to microseconds on write is the fix; nothing is lost because this
# data has second-level resolution at best. allow_truncated_timestamps silences
# the warning that coercion *could* discard precision.
#
# This is toolchain drift, not a logic change — the original Bronze files were
# written by an older pandas/pyarrow that defaulted to microseconds.
PARQUET_TIMESTAMP_OPTS = {"coerce_timestamps": "us", "allow_truncated_timestamps": True}


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write Parquet in a form Spark 3.5.1 can actually read."""
    df.to_parquet(path, index=False, **PARQUET_TIMESTAMP_OPTS)


def write_dataframe(df: pd.DataFrame, path_without_ext: Path) -> None:
    # Strict Parquet only. Install pyarrow with: uv add pyarrow
    write_parquet(df, path_without_ext.with_suffix(".parquet"))


def write_jsonl(records: List[dict], path: Path) -> None:
    """Write one JSON event per line so Kafka replay can stream them easily."""
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def generate_policyholders(config: GeneratorConfig) -> pd.DataFrame:
    """Create customer/policyholder master data with geography skew and schema evolution."""
    n = config.n_customers
    # Province distribution intentionally creates skew for Spark optimization discussion.
    provinces = np.random.choice(["QC", "ON"], size=n, p=[config.qc_ratio, config.ontario_ratio])

    cities = np.empty(n, dtype=object)
    qc_mask = provinces == "QC"
    on_mask = provinces == "ON"
    cities[qc_mask] = weighted_choice(QC_CITY_WEIGHTS, int(qc_mask.sum()))
    cities[on_mask] = weighted_choice(ON_CITY_WEIGHTS, int(on_mask.sum()))

    signup_ts = random_timestamps(window(config)["start"], config.days_history, n)

    df = pd.DataFrame({
        "customer_id": [f"cust_{i:06d}" for i in range(1, n + 1)],
        "signup_ts": signup_ts,
        "province": provinces,
        "city": cities,
        "age": np.random.randint(18, 86, size=n),
        "risk_segment": np.random.choice(["low", "medium", "high"], size=n, p=[0.55, 0.35, 0.10]),
        "marketing_opt_in": np.random.choice([True, False], size=n, p=[0.62, 0.38]),
    })

    # Simulate schema evolution: older source records did not have risk_segment.
    df.loc[df["signup_ts"] < window(config)["schema_change"], "risk_segment"] = None
    return df


def generate_policies(policyholders: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
    """Create insurance policies. One customer can own multiple policies."""
    n = config.n_policies
    # replace=True means the same customer may have multiple policies, which is realistic.
    customer_ids = np.random.choice(policyholders["customer_id"].values, size=n, replace=True)
    policy_types = np.random.choice(
        ["auto", "home", "tenant", "travel"],
        size=n,
        p=[config.auto_policy_ratio, 0.18, 0.04, 0.03],
    )

    start_dates = random_timestamps(window(config)["start"], config.days_history, n).dt.normalize()
    end_dates = start_dates + pd.to_timedelta(np.random.choice([180, 365], size=n, p=[0.15, 0.85]), unit="D")

    base_premium = {"auto": 1200, "home": 950, "tenant": 350, "travel": 180}
    premiums = [round(max(base_premium[p] * np.random.normal(1.0, 0.22), 80), 2) for p in policy_types]

    return pd.DataFrame({
        "policy_id": [f"pol_{i:07d}" for i in range(1, n + 1)],
        "customer_id": customer_ids,
        "policy_type": policy_types,
        "policy_start_date": start_dates,
        "policy_end_date": end_dates,
        "premium_amount": premiums,
        "policy_status": np.random.choice(["active", "expired", "cancelled"], size=n, p=[0.78, 0.15, 0.07]),
    })


def generate_claims(policies: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
    """Create claim facts and intentionally duplicate some rows for dedup testing."""
    n_claims = int(len(policies) * 0.18)
    sampled = policies.sample(n=n_claims, replace=False, random_state=config.random_seed).reset_index(drop=True)

    claim_type_map = {
        "auto": ["collision", "theft", "glass_damage", "vandalism", "weather_damage"],
        "home": ["water_damage", "fire", "theft", "liability", "weather_damage"],
        "tenant": ["theft", "water_damage", "liability"],
        "travel": ["medical", "trip_cancellation", "lost_baggage"],
    }
    claim_scale_map = {"auto": 1800, "home": 3000, "tenant": 700, "travel": 550}

    claim_types = []
    claim_amounts = []
    for policy_type in sampled["policy_type"]:
        claim_types.append(random.choice(claim_type_map[policy_type]))
        # Gamma distribution gives a right-skewed amount pattern: many small claims, few large claims.
        amount = np.random.gamma(shape=2.1, scale=claim_scale_map[policy_type])
        claim_amounts.append(round(float(max(amount, 50)), 2))

    # A claim cannot precede the policy that covers it.
    #
    # claim_date used to be drawn independently over the same window as
    # policy_start_date, which put roughly half the claims before their own policy
    # started. That is impossible, and it also destroyed the "claim filed soon
    # after policy start" fraud signal: 79% of claims looked like new-policy claims
    # because the elapsed time was negative. Each claim date is now drawn uniformly
    # between its policy's start and the end of the window.
    window_end = window(config)["end"]
    policy_starts = sampled["policy_start_date"]
    span_seconds = (window_end - policy_starts).dt.total_seconds().clip(lower=0)
    offsets = np.random.random(n_claims) * span_seconds.to_numpy()
    claim_dates = (
        policy_starts + pd.to_timedelta(offsets, unit="s")
    ).dt.normalize()

    df = pd.DataFrame({
        "claim_id": [f"clm_{i:07d}" for i in range(1, n_claims + 1)],
        "policy_id": sampled["policy_id"],
        "claim_date": claim_dates,
        "claim_type": claim_types,
        "claim_amount": claim_amounts,
        "claim_status": np.random.choice(["approved", "rejected", "pending"], size=n_claims, p=[0.72, 0.12, 0.16]),
    })


    # Data drift: claim severity inflates over the recent window.
    #
    # Applied to claim_amount because that is what propagates into the Gold
    # feature table (f_customer_avg_claim_amount_90d), which is what the drift
    # monitoring DAG actually compares. Drifting a column no feature reads would
    # be undetectable and therefore pointless.
    if config.drift_enabled:
        drift_mask = df["claim_date"] >= window(config)["drift_start"]
        df.loc[drift_mask, "claim_amount"] = (
            df.loc[drift_mask, "claim_amount"] * config.drift_claim_amount_multiplier
        ).round(2)
        print(
            f"  drift: inflated {int(drift_mask.sum())} claim amounts "
            f"by {config.drift_claim_amount_multiplier}x "
            f"(since {window(config)['drift_start'].date()})"
        )

    # Duplicate exact claim rows so Silver can prove deduplication works.
    n_duplicates = int(len(df) * config.duplicate_claim_rate)
    duplicate_rows = df.sample(n=n_duplicates, random_state=config.random_seed + 1).copy()
    return pd.concat([df, duplicate_rows], ignore_index=True)


def generate_payments(policies: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
    """Create payment attempts, including failures and old rows missing payment_method."""
    n_payments = int(len(policies) * 2.0)
    sampled = policies.sample(n=n_payments, replace=True, random_state=config.random_seed + 2).reset_index(drop=True)
    payment_dates = random_timestamps(window(config)["start"], config.days_history, n_payments)

    # Failure probability is per-row rather than a single np.random.choice, so the
    # drift window can carry a different rate. This one surfaces in the Gold
    # feature f_customer_payment_failure_rate_90d.
    failure_prob = np.full(n_payments, 0.08)
    if config.drift_enabled:
        drift_mask = (payment_dates >= window(config)["drift_start"]).to_numpy()
        failure_prob[drift_mask] += config.drift_payment_failure_uplift
        print(
            f"  drift: raised payment-failure rate by "
            f"{config.drift_payment_failure_uplift:+.2f} on {int(drift_mask.sum())} payments"
        )
    statuses = np.where(
        np.random.random(n_payments) < failure_prob, "failed", "success"
    )
    amounts = [round(float(premium / 12), 2) if status == "success" else 0.0 for premium, status in zip(sampled["premium_amount"], statuses)]

    df = pd.DataFrame({
        "payment_id": [f"pay_{i:08d}" for i in range(1, n_payments + 1)],
        "policy_id": sampled["policy_id"],
        "payment_date": payment_dates,
        "amount": amounts,
        "payment_method": np.random.choice(PAYMENT_METHODS, size=n_payments),
        "payment_status": statuses,
    })

    # Simulate schema evolution: old payment records did not include payment_method.
    df.loc[df["payment_date"] < window(config)["schema_change"], "payment_method"] = None
    return df


def generate_claim_labels(
    claims: pd.DataFrame,
    policies: pd.DataFrame,
    policyholders: pd.DataFrame,
    config: GeneratorConfig,
) -> pd.DataFrame:
    """
    Produce the fraud label table: one row per claim, `claim_id` and `is_fraud`.

    Labels come from a logistic model over real claim attributes, not a coin flip.
    That matters: random labels are unlearnable by construction, so every model
    would score ~0.5 AUC and the entire ML section would measure nothing. Here the
    drivers are deliberately ones the feature store also exposes, so a model has a
    genuine signal to find:

      - claim-to-premium ratio (a large claim against a small premium)
      - fraud-prone claim types (theft, vandalism, lost baggage)
      - a claim filed soon after the policy started
      - the customer's risk segment
      - weekend filing

    The intercept is solved numerically so the realised positive rate matches
    `fraud_base_rate` regardless of how the weights are tuned — otherwise changing
    any weight would silently move class balance and make runs incomparable.

    Labels are written as their own table rather than joined into Gold, because a
    label is not a feature: it arrives later, from a different process, and must
    never be materialised into the online store where it could leak into serving.
    """
    # Duplicate claim rows exist on purpose for Silver's dedup test; a label table
    # must be one row per claim.
    unique_claims = claims.drop_duplicates(subset=["claim_id"]).copy()

    enriched = unique_claims.merge(
        policies[["policy_id", "customer_id", "premium_amount", "policy_start_date"]],
        on="policy_id",
        how="left",
    ).merge(
        policyholders[["customer_id", "risk_segment"]],
        on="customer_id",
        how="left",
    )

    ratio = enriched["claim_amount"] / enriched["premium_amount"].clip(lower=1.0)
    # Log-scaled and standardised: the raw ratio is heavy-tailed, so a few huge
    # claims would otherwise dominate the logit and flatten everything else.
    log_ratio = np.log1p(ratio)
    ratio_z = ((log_ratio - log_ratio.mean()) / log_ratio.std(ddof=0)).fillna(0.0)

    days_since_policy_start = (
        enriched["claim_date"] - enriched["policy_start_date"]
    ).dt.days.fillna(999)
    new_policy = (days_since_policy_start <= 30).astype(float)

    fraud_prone_type = enriched["claim_type"].isin(config.fraud_prone_claim_types).astype(float)
    # risk_segment is null for pre-schema-change customers; absent is not high-risk.
    high_risk = (enriched["risk_segment"] == "high").fillna(False).astype(float)
    weekend = (enriched["claim_date"].dt.dayofweek >= 5).astype(float)

    logit = (
        config.fraud_high_ratio_weight * ratio_z
        + config.fraud_type_weight * fraud_prone_type
        + config.fraud_new_policy_weight * new_policy
        + config.fraud_high_risk_weight * high_risk
        + config.fraud_weekend_weight * weekend
    # copy=True because pandas copy-on-write hands back a read-only view, and the
    # drift adjustment below mutates this array in place.
    ).to_numpy(dtype=float, copy=True)

    if config.drift_enabled:
        # Concept drift: the same claim profile becomes likelier to be fraud in the
        # recent window. Expressed as a multiplier on the odds, so it shifts
        # prevalence without flattening the relationship between features and label.
        drift_mask = (enriched["claim_date"] >= window(config)["drift_start"]).to_numpy()
        logit[drift_mask] += np.log(config.drift_fraud_odds_multiplier)

    intercept = _solve_intercept(logit, config.fraud_base_rate)
    probability = sigmoid(intercept + logit)
    is_fraud = (np.random.random(len(probability)) < probability).astype(int)

    labels = pd.DataFrame(
        {
            "claim_id": enriched["claim_id"].to_numpy(),
            "is_fraud": is_fraud,
            # Kept for the training pipeline's point-in-time join: a label is only
            # knowable after the claim, so this is the earliest honest timestamp.
            "label_ts": enriched["claim_date"].to_numpy(),
        }
    )

    realised = labels["is_fraud"].mean()
    print(
        f"claim_labels: rows={len(labels)} fraud_rate={realised:.4f} "
        f"(target {config.fraud_base_rate:.4f})"
    )
    return labels


def _solve_intercept(logit: np.ndarray, target_rate: float) -> float:
    """
    Bisect for the intercept that makes mean(sigmoid(intercept + logit)) == target.

    Keeps class balance pinned to `fraud_base_rate` no matter how the feature
    weights are set, so weight changes alter *which* claims are fraudulent without
    also moving prevalence.
    """
    low, high = -20.0, 20.0
    for _ in range(200):
        mid = (low + high) / 2.0
        if sigmoid(mid + logit).mean() < target_rate:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def is_burst_minute(timestamp: pd.Timestamp, burst_windows: Tuple[str, str]) -> bool:
    """Return True when the current minute falls inside a configured burst window."""
    current_hhmm = timestamp.strftime("%H:%M")
    for window in burst_windows:
        start, end = window.split("-")
        if start <= current_hhmm < end:
            return True
    return False


def generate_streaming_events(policyholders: pd.DataFrame, policies: pd.DataFrame, config: GeneratorConfig, stream_days: int = 1) -> List[dict]:
    """Create JSON-style events with bursts, late arrivals, and duplicate event IDs."""
    customer_geo = policyholders.set_index("customer_id")[["province", "city"]].to_dict("index")
    policies_by_customer = policies.groupby("customer_id")["policy_id"].apply(list).to_dict()
    all_customer_ids = policyholders["customer_id"].values

    # Anchored to the window like every other table, so streaming and offline
    # data cannot end up describing different months.
    start_ts = window(config)["end"] - pd.Timedelta(days=stream_days)
    end_ts = start_ts + pd.Timedelta(days=stream_days)
    event_types = ["quote_view", "quote_started", "policy_purchase", "claim_submitted", "payment_attempt", "payment_failed"]
    event_probs = [0.42, 0.22, 0.08, 0.08, 0.16, 0.04]

    events = []
    event_counter = 1
    current = start_ts

    while current < end_ts:
        normal_volume = config.base_events_per_min
        burst_volume = config.base_events_per_min * config.burst_multiplier
        # Burst windows create traffic spikes that streaming systems must handle.
        events_this_minute = burst_volume if is_burst_minute(current, config.burst_windows) else normal_volume
        events_this_minute = max(1, int(np.random.normal(events_this_minute, events_this_minute * 0.08)))

        customer_ids = np.random.choice(all_customer_ids, size=events_this_minute, replace=True)
        chosen_event_types = np.random.choice(event_types, size=events_this_minute, p=event_probs)

        for customer_id, event_type in zip(customer_ids, chosen_event_types):
            customer_id = str(customer_id)
            event_type = str(event_type)
            policy_id = None
            customer_policies = policies_by_customer.get(customer_id, [])
            if customer_policies and event_type in ["policy_purchase", "claim_submitted", "payment_attempt", "payment_failed"]:
                policy_id = str(np.random.choice(customer_policies))

            event_timestamp = current + pd.Timedelta(seconds=int(np.random.randint(0, 60)))
            # event_timestamp = when the business event happened.
            # created_ts = when the event arrived in the pipeline.
            # A later created_ts simulates late-arriving streaming data.
            if np.random.random() < config.late_arrival_rate:
                delay = int(np.random.randint(config.late_delay_min_max[0], config.late_delay_min_max[1] + 1))
                created_ts = event_timestamp + pd.Timedelta(minutes=delay)
            else:
                created_ts = event_timestamp + pd.Timedelta(seconds=int(np.random.randint(0, 60)))

            geo = customer_geo[customer_id]
            events.append({
                "event_id": f"evt_{event_counter:09d}",
                "event_type": event_type,
                "event_timestamp": event_timestamp.isoformat(),
                "created_ts": created_ts.isoformat(),
                "customer_id": customer_id,
                "policy_id": policy_id,
                "province": geo["province"],
                "city": geo["city"],
                "channel": str(np.random.choice(CHANNELS, p=[0.36, 0.34, 0.16, 0.14])),
                "device_type": str(np.random.choice(DEVICE_TYPES, p=[0.44, 0.48, 0.08])),
            })
            event_counter += 1

        current += pd.Timedelta(minutes=1)

    # Duplicate event_id with a later created_ts to simulate replay/retry duplicates.
    n_duplicates = int(len(events) * config.duplicate_stream_rate)
    duplicate_indices = np.random.choice(len(events), size=n_duplicates, replace=False)
    duplicate_events = []
    for idx in duplicate_indices:
        dup = events[idx].copy()
        dup["created_ts"] = (pd.Timestamp(dup["created_ts"]) + pd.Timedelta(minutes=int(np.random.randint(1, 4)))).isoformat()
        duplicate_events.append(dup)

    events.extend(duplicate_events)
    events.sort(key=lambda row: row["created_ts"])
    return events


def build_quality_report(policyholders: pd.DataFrame, policies: pd.DataFrame, claims: pd.DataFrame, payments: pd.DataFrame, events: List[dict]) -> Dict[str, object]:
    """Summarize generated data issues so the Part 01 deliverable has evidence."""
    events_df = pd.DataFrame(events)
    return {
        "policyholder_count": int(len(policyholders)),
        "policy_count": int(len(policies)),
        "claim_count_including_duplicates": int(len(claims)),
        "payment_count": int(len(payments)),
        "province_distribution_pct": policyholders["province"].value_counts(normalize=True).mul(100).round(2).to_dict(),
        "top_city_distribution_pct": policyholders["city"].value_counts(normalize=True).head(10).mul(100).round(2).to_dict(),
        "policy_type_distribution_pct": policies["policy_type"].value_counts(normalize=True).mul(100).round(2).to_dict(),
        "customer_id_distinct_count": int(policyholders["customer_id"].nunique()),
        "policy_id_distinct_count": int(policies["policy_id"].nunique()),
        "claim_id_distinct_count": int(claims["claim_id"].nunique()),
        "claim_duplicate_id_count": int(len(claims) - claims["claim_id"].nunique()),
        "claim_duplicate_id_rate_pct": round(
            claims.duplicated(subset=["claim_id"], keep=False).mean() * 100,
            2,
        ),
        "risk_segment_null_rate_pct": round(policyholders["risk_segment"].isna().mean() * 100, 2),
        "payment_method_null_rate_pct": round(payments["payment_method"].isna().mean() * 100, 2),
        "claim_duplicate_rate_by_business_key_pct": round(claims.duplicated(subset=["policy_id", "claim_date", "claim_type", "claim_amount"], keep=False).mean() * 100, 2),
        "stream_event_count_including_duplicates": int(len(events_df)),
        "stream_event_type_distribution_pct": events_df["event_type"].value_counts(normalize=True).mul(100).round(2).to_dict(),
        "stream_duplicate_event_id_rate_pct": round(events_df.duplicated(subset=["event_id"], keep=False).mean() * 100, 2),
        "stream_late_arrival_rate_pct": round((pd.to_datetime(events_df["created_ts"]) > pd.to_datetime(events_df["event_timestamp"]) + pd.Timedelta(minutes=5)).mean() * 100, 2),
    }


def main(config: GeneratorConfig = CONFIG) -> None:
    set_random_seed(config.random_seed)
    output_dir = Path(config.output_dir)
    offline_dir = output_dir / "offline"
    streaming_dir = output_dir / "streaming"
    report_dir = output_dir / "reports"
    offline_dir.mkdir(parents=True, exist_ok=True)
    streaming_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    print("Generating policyholders...")
    policyholders = generate_policyholders(config)
    print("Generating policies...")
    policies = generate_policies(policyholders, config)
    print("Generating claims...")
    claims = generate_claims(policies, config)
    print("Generating payments...")
    payments = generate_payments(policies, config)
    print("Generating fraud labels...")
    claim_labels = generate_claim_labels(claims, policies, policyholders, config)
    print("Generating streaming-style events...")
    events = generate_streaming_events(policyholders, policies, config, stream_days=1)

    print("Writing strict Parquet offline datasets...")
    
    # Create two physical Parquet files with different schemas for policyholders to simulate schema evolution. The old file has no risk_segment column.
    policyholders_dir = offline_dir / "policyholders"
    policyholders_dir.mkdir(parents=True, exist_ok=True)

    old_ph = policyholders[
        policyholders["signup_ts"] < window(config)["schema_change"]
    ].drop(columns=["risk_segment"])

    new_ph = policyholders[
        policyholders["signup_ts"] >= window(config)["schema_change"]
    ]

    write_parquet(old_ph, policyholders_dir / "part_old.parquet")
    write_parquet(new_ph, policyholders_dir / "part_new.parquet")

    # Write the other tables as single Parquet files.
    write_dataframe(policies, offline_dir / "policies")
    write_dataframe(claims, offline_dir / "claims")
    write_dataframe(payments, offline_dir / "payments")
    # The label table sits beside the feature sources but is deliberately its own
    # file: it is not a feature, arrives from a different process, and must never
    # reach the online store.
    write_dataframe(claim_labels, offline_dir / "claim_labels")

    print("Writing streaming JSONL...")
    write_jsonl(events, streaming_dir / "insurance_events.jsonl")

    print("Writing config and quality report...")
    with (output_dir / "generator_config.json").open("w", encoding="utf-8") as f:
        json.dump(asdict(config), f, indent=2)
    report = build_quality_report(policyholders, policies, claims, payments, events)
    with (report_dir / "quality_report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\nDone.")
    print(f"Policyholders: {len(policyholders):,}")
    print(f"Policies: {len(policies):,}")
    print(f"Claims incl. duplicates: {len(claims):,}")
    print(f"Payments: {len(payments):,}")
    print(f"Claim labels: {len(claim_labels):,} (fraud rate {claim_labels['is_fraud'].mean():.4f})")
    print(f"Window: {window(config)['start'].date()} -> {window(config)['end'].date()}")
    if config.drift_enabled:
        print(f"Drift window starts: {window(config)['drift_start'].date()}")
    print(f"Streaming events incl. duplicates: {len(events):,}")
    print(f"Output folder: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
