from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import httpx
from feast import FeatureStore

from ml import config


class FeatureLookupError(RuntimeError):
    pass


class InferenceError(RuntimeError):
    pass


class FeastOnlineFeatureRepository:
    def __init__(self, store: FeatureStore, feature_refs: list[str] | None = None):
        self._store = store
        self._feature_refs = feature_refs or config.FEATURE_REFS

    async def get_features(self, claim_id: str, customer_id: str) -> dict[str, Any]:
        entity_row = {"claim_id": claim_id, "customer_id": customer_id}
        result = await asyncio.to_thread(
            self._store.get_online_features,
            features=self._feature_refs,
            entity_rows=[entity_row],
        )
        values = result.to_dict()
        features = {
            name: column[0]
            for name, column in values.items()
            if name in config.MODEL_FEATURES
        }
        missing = [
            name for name in config.MODEL_FEATURES
            if name not in features or features[name] is None
        ]
        if missing:
            raise FeatureLookupError(
                f"online features missing for claim_id={claim_id}: {missing}"
            )
        return features


class KServeClient:
    def __init__(
        self,
        url: str,
        client: httpx.AsyncClient,
        model_name: str = "fraud-detector",
        model_version: str = "production",
    ):
        self._url = url.rstrip("/")
        self._client = client
        self.model_name = model_name
        self.model_version = model_version

    async def predict(self, features: Mapping[str, Any]) -> float:
        instances = [[features[name] for name in config.MODEL_FEATURES]]
        try:
            response = await self._client.post(
                self._url,
                json={"instances": instances},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise InferenceError("KServe prediction request failed") from exc

        payload = response.json()
        try:
            probability = float(payload["predictions"][0])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise InferenceError("KServe returned an invalid prediction payload") from exc
        if not 0.0 <= probability <= 1.0:
            raise InferenceError("KServe returned a probability outside [0, 1]")
        return probability
