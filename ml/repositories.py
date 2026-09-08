"""
Data access for the training path — the repository pattern of CLAUDE.md §15.

Every external system the training code reads sits behind a thin interface here:
Feast, the label Parquet, the claim spine, the Delta transaction log. The services
in `services.py` depend on the Protocols, never on `feast` or `pandas.read_parquet`
directly.

The payoff is not abstraction for its own sake. It is that a unit test can
substitute an in-memory repository and exercise the split, the training and the
evaluation logic with no GCS bucket, no Feast registry and no cluster — which is
what makes the >90% coverage target in §9 reachable at all. The Kubeflow
components in §5 get the same seam: a component can be handed a different
repository without touching the logic it wraps.

Interfaces are `Protocol`, not ABCs, so an implementation does not have to
import this module to satisfy one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd

from . import config


class ClaimSpineRepository(Protocol):
    """Which claims exist, and the instant each should be scored as of."""

    def load(self) -> pd.DataFrame:
        """Returns claim_id, customer_id, event_timestamp."""
        ...


class LabelRepository(Protocol):
    """The fraud outcome per claim, from whatever process produces labels."""

    def load(self) -> pd.DataFrame:
        """Returns claim_id, is_fraud."""
        ...


class FeatureRepository(Protocol):
    """Point-in-time-correct feature retrieval for a given entity dataframe."""

    def get_historical_features(
        self, entity_df: pd.DataFrame, feature_refs: list[str]
    ) -> pd.DataFrame:
        ...


class DataVersionRepository(Protocol):
    """The version identifier of the dataset a run read."""

    def current_version(self) -> int:
        ...


class ParquetClaimSpineRepository:
    """Claim spine from the Gold claims export on GCS."""

    def __init__(self, path: str = config.SPINE_PATH):
        self._path = path

    def load(self) -> pd.DataFrame:
        columns = config.ENTITY_KEYS + [config.EVENT_TIMESTAMP]
        return pd.read_parquet(self._path, columns=columns)


class ParquetLabelRepository:
    """Fraud labels from the Bronze label table."""

    def __init__(self, path: str = config.LABEL_PATH):
        self._path = path

    def load(self) -> pd.DataFrame:
        frame = pd.read_parquet(self._path, columns=["claim_id", config.LABEL])
        if not frame["claim_id"].is_unique:
            # One row per claim is the contract. Duplicates would silently
            # multiply rows in the join and weight those claims more heavily in
            # training, which no metric would reveal.
            duplicates = int(frame["claim_id"].duplicated().sum())
            raise ValueError(f"label table has {duplicates} duplicate claim_id rows")
        return frame


class FeastFeatureRepository:
    """
    Feature retrieval through the Feast SDK.

    The store is constructed lazily. Building a FeatureStore reads the registry
    from GCS, and doing that at import time would make this module impossible to
    import in a unit test.
    """

    def __init__(self, repo_path: str = config.FEAST_REPO_PATH):
        self._repo_path = repo_path
        self._store = None

    @property
    def store(self):
        if self._store is None:
            from feast import FeatureStore

            self._store = FeatureStore(repo_path=self._repo_path)
        return self._store

    def get_historical_features(
        self, entity_df: pd.DataFrame, feature_refs: list[str]
    ) -> pd.DataFrame:
        return self.store.get_historical_features(
            entity_df=entity_df, features=feature_refs
        ).to_df()

    def describe(self) -> dict:
        """Registry facts worth logging with a run: which views, which TTLs."""
        return {
            "project": self.store.project,
            "registry": self.store.config.registry.path,
            "views": {
                view.name: {"ttl_days": view.ttl.days, "features": len(view.features)}
                for view in self.store.list_feature_views()
            },
        }


class DeltaLogDataVersionRepository:
    """
    The Delta table version, read from the transaction log.

    The version is the highest numbered commit JSON in `_delta_log/`"""

    def __init__(self, table_path: str = config.DELTA_TABLE_PATH):
        self._table_path = table_path

    def current_version(self) -> int:
        import gcsfs

        prefix = self._table_path.removeprefix("gs://")
        fs = gcsfs.GCSFileSystem()
        commits = fs.glob(f"{prefix}/_delta_log/*.json")
        if not commits:
            raise FileNotFoundError(f"no Delta commits under {self._table_path}/_delta_log")
        return max(int(Path(commit).stem) for commit in commits)
