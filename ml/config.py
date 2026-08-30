"""
Canonical configuration for the training path.

This module is the single definition of *what the model is trained on*. The
notebook (`notebooks/01_fraud_model_baseline.ipynb`) was the exploration that
produced these choices and states them inline for readability; from here on this
file is authoritative, and the Kubeflow components in §5 read the same constants
so a pipeline run and a local run cannot silently disagree about the feature set.

Paths are environment-driven with the deployed values as defaults, so the same
code runs in a training pod, in a Kubeflow component, or on a laptop with
`gcloud auth application-default login`.
"""

from __future__ import annotations

import os

LAKEHOUSE = (os.getenv("LAKEHOUSE_ROOT") or "gs://aide-playground-lakehouse").rstrip("/")

# The Feast repo definition (feature_store.yaml + features.py). Baked into the
# training image so a run cannot pick up a stale definition, while the registry
# it points at stays in GCS and is shared with the Materialize Pipeline.
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/feature_store")

# Labels live in Bronze, never in Gold or a feature view: a label arrives from a
# different process than the features, and anything in a feature view is
# materialized into Redis where the prediction API could read it.
LABEL_PATH = f"{LAKEHOUSE}/bronze/offline/claim_labels.parquet"

# Entity spine: which claims exist, and when. Entity keys and a timestamp, not
# features — this is the question put to Feast, not an answer from it.
SPINE_PATH = f"{LAKEHOUSE}/feast/obt_claims_enriched"

# The Delta copy of the same Gold table. Read only for its transaction-log
# version, which is logged as an MLflow tag so a registered model names the exact
# data snapshot it was trained on (§7).
DELTA_TABLE_PATH = f"{LAKEHOUSE}/delta/gold/obt_claims_enriched"

ENTITY_KEYS = ["claim_id", "customer_id"]
EVENT_TIMESTAMP = "event_timestamp"
LABEL = "is_fraud"

# ---------------------------------------------------------------------------
# Feature references, by view. Requested from Feast as a point-in-time join at
# each claim's own filing date.
# ---------------------------------------------------------------------------

CLAIM_FEATURE_REFS = [
    "claim_features:claim_amount",
    "claim_features:premium_amount",
    "claim_features:claim_to_premium_ratio",
    "claim_features:days_since_policy_start",
    "claim_features:claim_type",
    "claim_features:policy_type",
    "claim_features:policy_status",
    "claim_features:province",
    "claim_features:risk_segment",
    "claim_features:age",
    "claim_features:marketing_opt_in",
    "claim_features:claim_month",
    "claim_features:claim_day_of_week",
    "claim_features:claim_is_weekend",
]

CUSTOMER_FEATURE_REFS = [
    "customer_90d:f_customer_avg_claim_amount_90d",
    "customer_90d:f_customer_total_claims_90d",
    "customer_90d:f_customer_total_claim_amount_90d",
    "customer_90d:f_customer_total_payments_90d",
    "customer_90d:f_customer_payment_failure_rate_90d",
]

FEATURE_REFS = CLAIM_FEATURE_REFS + CUSTOMER_FEATURE_REFS

# ---------------------------------------------------------------------------
# Model input. Retrieved != used: two retrieved features are deliberately not
# fed to the estimator.
# ---------------------------------------------------------------------------

NUMERIC = [
    "claim_amount",
    "premium_amount",
    "claim_to_premium_ratio",
    "days_since_policy_start",
    "age",
    "claim_day_of_week",
    "f_customer_avg_claim_amount_90d",
    "f_customer_total_claims_90d",
    "f_customer_total_claim_amount_90d",
    "f_customer_total_payments_90d",
    "f_customer_payment_failure_rate_90d",
]

BINARY = ["marketing_opt_in", "claim_is_weekend"]

CATEGORICAL = ["claim_type", "policy_type", "policy_status", "province", "risk_segment"]

MODEL_FEATURES = NUMERIC + BINARY + CATEGORICAL

# Retrieved and then dropped, with the reason, because "why is this not a
# feature" is the question a reviewer asks and the code should answer:
#
#   claim_month  under a temporal split it is a proxy for the split itself.
#                Training sees months 5-7 and validation is month 8, so any
#                weight learned on it cannot generalize forward. claim_day_of_week
#                and claim_is_weekend stay: they cycle.
#
# (claim_status is excluded one level up, from the feature view itself — it
# records the outcome of claim processing, decided after any fraud assessment.)
EXCLUDED_FROM_MODEL = ["claim_month"]

# ---------------------------------------------------------------------------
# Training policy
# ---------------------------------------------------------------------------

# Split by time, not at random. Production only ever scores the future; the
# generator's drift window sits in the last 14 days; and one customer's claims
# are correlated through the customer_90d features. A random split violates all
# three and reports a better number than the first day of serving would.
SPLIT_QUANTILE = float(os.getenv("SPLIT_QUANTILE", "0.8"))

# The operating point is a review capacity, not a probability. Nobody
# investigates every claim, so the question is what a team reviewing the top N%
# by risk sees — and a capacity survives the model being poorly calibrated.
REVIEW_BUDGET = float(os.getenv("REVIEW_BUDGET", "0.10"))
BUDGET_CURVE = (0.05, 0.10, 0.20, 0.30)

RANDOM_STATE = 42

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------

TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.ml-ns.svc.cluster.local:5000")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "insurance-fraud")
REGISTERED_MODEL = os.getenv("MLFLOW_REGISTERED_MODEL", "fraud-detector")

# MLflow 3 removed model-version stages, so the pointer serving reads is an
# alias. Moving an alias between versions is atomic, which is what makes it the
# right primitive for §13's champion/challenger promotion as well.
PRODUCTION_ALIAS = os.getenv("MLFLOW_PRODUCTION_ALIAS", "production")
