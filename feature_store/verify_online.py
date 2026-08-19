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


def check(store: FeatureStore, name: str, rows: list[dict], features: list[str]) -> bool:
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
        ) and ok
    else:
        print("\nclaim_features: FAIL — could not read any claim_id from the offline source")
        ok = False

    if not ok:
        print("\nOnline store verification FAILED")
        sys.exit(1)
    print("\nOnline store verification PASSED")


def _sample_claim_ids(store: FeatureStore, limit: int = 3) -> list[str]:
    """Pull a few real claim_ids straight from the Parquet the view is built on."""
    source_path = None
    for view in store.list_feature_views():
        if view.name == "claim_features":
            source_path = view.batch_source.path
            break
    if not source_path:
        return []

    # gcsfs is installed for Feast's own gs:// access, so pandas can read the
    # directory of part files directly.
    frame = pd.read_parquet(source_path, columns=["claim_id"])
    return frame["claim_id"].head(limit).tolist()


if __name__ == "__main__":
    main()
