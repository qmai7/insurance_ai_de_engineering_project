"""
Verify the online store actually serves features after materialization.

`feast materialize` reporting success is not proof that a read works: the registry
can be applied, the job can report rows written, and online reads can still come
back empty — wrong entity key serialization, a TTL that excludes every row, a
Redis pointed at the wrong namespace. Each of those looks identical to a healthy
store until the prediction API starts returning nothing for every request.

So this reads real entity keys back out and fails loudly if the values are
missing. It is the last task of the Materialize Pipeline for that reason.
"""

from __future__ import annotations

import sys

import pandas as pd
from feast import FeatureStore

# Deliberately small and fixed. The generator is seeded, so these IDs exist in
# every regenerated dataset and the check is reproducible.
CUSTOMER_IDS = ["cust_000001", "cust_000002", "cust_000003"]

CUSTOMER_FEATURES = [
    "customer_90d:f_customer_avg_claim_amount_90d",
    "customer_90d:f_customer_total_claims_90d",
    "customer_90d:f_customer_total_claim_amount_90d",
    "customer_90d:f_customer_total_payments_90d",
    "customer_90d:f_customer_payment_failure_rate_90d",
]

CLAIM_FEATURES = [
    "claim_features:claim_amount",
    "claim_features:claim_to_premium_ratio",
    "claim_features:claim_type",
]


def check(
    store: FeatureStore,
    name: str,
    rows: list[dict],
    features: list[str],
    entity_key: str,
    source_path: str | None = None,
) -> bool:
    result = store.get_online_features(features=features, entity_rows=rows).to_dict()
    frame = pd.DataFrame(result)
    print(f"\n{name}:")
    print(frame.to_string(index=False))

    # A miss in Feast is a None, not an error — so "did it return rows" is not the
    # question. Whether every requested feature came back populated is.
    feature_cols = [f.split(":")[1] for f in features]
    all_null = [c for c in feature_cols if frame[c].isna().all()]
    if all_null:
        print(f"  FAIL: no values for {all_null}")
        return False

    print(f"  OK: {len(feature_cols)} features populated for {len(rows)} entities")

    if source_path is None:
        return True

    # Populated is not the same as current.
    #
    # Redis persists across runs, so a materialization that loads nothing at all
    # still leaves the previous run's values in place and the check above passes —
    # a false green that hid exactly this bug once already: as_of_date was pinned
    # to a stale literal, incremental materialization loaded zero customer rows,
    # and verification still reported success on months-old values.
    #
    # So compare the served values against the offline source they are supposed to
    # come from. Any drift between them means the online store is stale.
    numeric_cols = [
        c for c in feature_cols if pd.api.types.is_numeric_dtype(frame[c])
    ]
    if not numeric_cols:
        return True

    offline = pd.read_parquet(source_path, columns=[entity_key] + numeric_cols)
    # Latest row per entity, matching what the online store should hold.
    offline = offline.drop_duplicates(subset=[entity_key], keep="last")
    merged = frame[[entity_key] + numeric_cols].merge(
        offline, on=entity_key, how="inner", suffixes=("_online", "_offline")
    )
    if merged.empty:
        print(f"  FAIL: none of the sampled {entity_key}s exist in the offline source")
        return False

    stale = []
    for col in numeric_cols:
        online_vals = merged[f"{col}_online"].astype(float)
        offline_vals = merged[f"{col}_offline"].astype(float)
        if not (online_vals - offline_vals).abs().le(1e-6).all():
            stale.append(col)

    if stale:
        print(f"  FAIL: online values disagree with the offline source for {stale}")
        print("        the online store is serving stale features")
        return False

    print(f"  OK: {len(numeric_cols)} numeric features match the offline source")
    return True


def main() -> None:
    store = FeatureStore(repo_path=".")
    print(f"project={store.project} registry={store.config.registry.path}")
    print(f"online_store={store.config.online_store.connection_string}")

    ok = check(
        store,
        "customer_90d",
        [{"customer_id": cid} for cid in CUSTOMER_IDS],
        CUSTOMER_FEATURES,
        entity_key="customer_id",
        source_path=_source_path(store, "customer_90d"),
    )

    # Claim IDs are not guessable across regenerations the way customer IDs are,
    # so they are read from the offline Parquet rather than hardcoded.
    claim_ids = _sample_claim_ids(store)
    if claim_ids:
        ok = check(
            store,
            "claim_features",
            [{"claim_id": cid} for cid in claim_ids],
            CLAIM_FEATURES,
            entity_key="claim_id",
            source_path=_source_path(store, "claim_features"),
        ) and ok
    else:
        print("\nclaim_features: FAIL — could not read any claim_id from the offline source")
        ok = False

    if not ok:
        print("\nOnline store verification FAILED")
        sys.exit(1)
    print("\nOnline store verification PASSED")


def _source_path(store: FeatureStore, view_name: str) -> str | None:
    """The Parquet path backing a view, read from the registry rather than guessed."""
    for view in store.list_feature_views():
        if view.name == view_name:
            return view.batch_source.path
    return None


def _sample_claim_ids(store: FeatureStore, limit: int = 3) -> list[str]:
    """Pull a few real claim_ids straight from the Parquet the view is built on."""
    source_path = _source_path(store, "claim_features")
    if not source_path:
        return []

    # gcsfs is installed for Feast's own gs:// access, so pandas can read the
    # directory of part files directly.
    frame = pd.read_parquet(source_path, columns=["claim_id"])
    return frame["claim_id"].head(limit).tolist()


if __name__ == "__main__":
    main()
