"""
Verify the online store actually serves every feature the registry declares.

`feast materialize` reporting success is not proof that a read works. The registry
can be applied, the job can report progress, and online reads can still come back
empty or stale — wrong entity key serialization, a TTL that excludes every row, a
Redis pointed at the wrong namespace, or a write the online store silently declined.
Each of those looks identical to a healthy store until the prediction API starts
returning nothing.

So this compares the online store against the offline source it was materialized
from, entity by entity and feature by feature. Three distinct failures, all silent
in production:

  MISSING  offline has a value, online has null   -> that feature never reached Redis
  STALE    both have values, and they disagree    -> Redis holds an older write
  ABSENT   no feature has a value online          -> the entity was never materialized

The feature list is read from the registry rather than hardcoded, which is the
point: a feature added to a view is verified automatically. A hardcoded list is
exactly how `days_since_policy_start` was added to `claim_features`, materialized,
and served as null with the pipeline reporting success end to end.
"""

from __future__ import annotations

import sys

import pandas as pd
from feast import FeatureStore

# Deliberately small and fixed. Enough entities that a feature which is legitimately
# null for some rows (risk_segment, absent for pre-schema-change customers) is not
# mistaken for one that is missing everywhere, and few enough that this stays a
# seconds-long pipeline step.
SAMPLE_SIZE = 25

TOLERANCE = 1e-6


def view_spec(store: FeatureStore, view_name: str) -> dict:
    """
    Everything needed to check a view, read from the registry rather than guessed.

    Guessing is what lets this drift: a check that names its own features cannot
    fail when the view gains one.
    """
    view = next(v for v in store.list_feature_views() if v.name == view_name)
    entity = store.get_entity(view.entities[0])
    return {
        "name": view.name,
        "features": [f.name for f in view.features],
        "entity_key": entity.join_key,
        "source_path": view.batch_source.path,
        "timestamp_field": view.batch_source.timestamp_field,
        "ttl_days": view.ttl.days,
    }


def offline_latest(spec: dict) -> pd.DataFrame:
    """
    The row per entity that the online store is supposed to be holding.

    Sorted by the source's own event timestamp, not by position in the Parquet.
    feat_customer_90d is a time series with many rows per customer and Spark's
    part-file order is arbitrary, so taking whichever row happens to come last
    would compare the online value against a random point in that customer's
    history and report drift that is not there.
    """
    frame = pd.read_parquet(
        spec["source_path"],
        columns=[spec["entity_key"], spec["timestamp_field"]] + spec["features"],
    )
    return (
        frame.sort_values(spec["timestamp_field"])
        .drop_duplicates(subset=[spec["entity_key"]], keep="last")
        .drop(columns=[spec["timestamp_field"]])
        .reset_index(drop=True)
    )


def disagreements(online_row: pd.Series, offline_row: pd.Series, features: list[str]) -> list[str]:
    """Per-feature comparison, typed: numbers by tolerance, everything else by equality."""
    problems = []
    for feature in features:
        got, want = online_row[feature], offline_row[feature]

        if pd.isna(want):
            # Nothing to serve. A null online is the correct answer.
            continue
        if pd.isna(got):
            problems.append(f"MISSING {feature}")
            continue

        if isinstance(want, (int, float)) and isinstance(got, (int, float)):
            if abs(float(got) - float(want)) > TOLERANCE:
                problems.append(f"STALE {feature} (online={got} offline={want})")
        elif got != want:
            problems.append(f"STALE {feature} (online={got!r} offline={want!r})")
    return problems


def check_view(store: FeatureStore, view_name: str) -> bool:
    spec = view_spec(store, view_name)
    offline = offline_latest(spec)
    sample = offline.head(SAMPLE_SIZE)

    print(f"\n{spec['name']}: {len(spec['features'])} features, ttl={spec['ttl_days']}d")
    print(f"  offline source {spec['source_path']} ({len(offline)} entities)")

    entity_key = spec["entity_key"]
    online = store.get_online_features(
        features=[f"{spec['name']}:{f}" for f in spec["features"]],
        entity_rows=[{entity_key: v} for v in sample[entity_key]],
    ).to_df()

    online = online.set_index(entity_key)
    failures = []
    for _, offline_row in sample.iterrows():
        key = offline_row[entity_key]

        # A miss is not an omission. Feast always returns a row per requested
        # entity and fills it with nulls, so "no online row" and "every feature
        # null" are the same observation — collapse them into one line instead of
        # repeating MISSING once per feature, which is what a deleted key produced.
        expected = [f for f in spec["features"] if not pd.isna(offline_row[f])]
        if key not in online.index or all(pd.isna(online.loc[key][f]) for f in expected):
            failures.append(f"  ABSENT {entity_key}={key} (no online values for {len(expected)} features)")
            continue

        for problem in disagreements(online.loc[key], offline_row, spec["features"]):
            failures.append(f"  {problem}  [{entity_key}={key}]")

    if failures:
        print(f"  FAIL: {len(failures)} problem(s) across {len(sample)} entities")
        # Capped: one broken feature produces one line per sampled entity, and the
        # first few say everything the rest would.
        for line in failures[:10]:
            print(line)
        if len(failures) > 10:
            print(f"  ... and {len(failures) - 10} more")
        return False

    print(f"  OK: {len(spec['features'])} features match the offline source "
          f"for {len(sample)} entities")
    return True


def main() -> None:
    store = FeatureStore(repo_path=".")
    print(f"project={store.project} registry={store.config.registry.path}")
    print(f"online_store={store.config.online_store.connection_string}")

    views = [v.name for v in store.list_feature_views()]
    print(f"verifying {len(views)} view(s) from the registry: {views}")

    ok = all([check_view(store, name) for name in views])

    if not ok:
        print(
            "\nOnline store verification FAILED\n"
            "\nIf a feature is MISSING everywhere, the usual cause is that the view "
            "gained a feature while Redis still holds keys from an earlier write. "
            "Feast declines a write whose event timestamp is not newer than the "
            "stored one, so re-exporting unchanged data updates nothing — schema "
            "included. See docs/feature_store.md."
        )
        sys.exit(1)
    print("\nOnline store verification PASSED")


if __name__ == "__main__":
    main()
