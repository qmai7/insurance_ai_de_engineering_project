from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx
from feast import FeatureStore

from ml import config


class FeatureLookupError(RuntimeError):
    pass


class InferenceError(RuntimeError):
    pass


@dataclass(frozen=True)
class Prediction:
    """
    A score plus which model produced it.

    The attribution is not decoration. §13 splits traffic between champion and
    challenger behind one Service name, so the API cannot know from its own
    config which model answered a given request — only the response can tell it.
    Returning the two together makes it impossible to log a probability without
    the version it came from.
    """

    probability: float
    model_role: str
    model_version: str


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


class ModelServerClient:
    """
    The API's only route to a model — §15's "model client" layer.

    It holds no model and knows no MLflow. That is the point: the API scales on
    request rate, the model-server scales on scoring cost, and champion/
    challenger traffic weights are set in the mesh between them without this
    class or the request layer changing at all.

    The wire format is KServe's v1 protocol (`{"instances": [[...]]}` in,
    `{"predictions": [...]}` out) even though KServe itself is not deployed —
    it is a published contract, so a real inference server can be substituted
    behind the same Service without touching the caller.
    """

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
        # The configured *expectation*. What actually answered comes back on the
        # response headers and wins — see `Prediction`.
        self.model_version = model_version

    async def predict(self, features: Mapping[str, Any]) -> Prediction:
        # Column order is `config.MODEL_FEATURES`, the same list the training
        # matrix is built from. Both sides reading one constant is what keeps
        # the positional payload from silently transposing two features.
        instances = [[features[name] for name in config.MODEL_FEATURES]]
        try:
            response = await self._client.post(
                self._url,
                json={"instances": instances},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise InferenceError("model-server prediction request failed") from exc

        payload = response.json()
        try:
            probability = float(payload["predictions"][0])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise InferenceError("model-server returned an invalid prediction payload") from exc
        if not 0.0 <= probability <= 1.0:
            raise InferenceError("model-server returned a probability outside [0, 1]")

        return Prediction(
            probability=probability,
            model_role=response.headers.get("x-model-role", "unknown"),
            model_version=response.headers.get("x-model-version", self.model_version),
        )
