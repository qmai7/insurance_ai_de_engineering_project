from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from ml import config
from ml.services import ModelBuilder, ModelRegistryService

logger = logging.getLogger(__name__)


class ModelNotLoadedError(RuntimeError):
    """Raised when a predict arrives before a model is in memory."""


class ScoringError(RuntimeError):
    """The model was loaded but could not score the given rows."""


@dataclass(frozen=True)
class LoadedModel:
    """A model plus the registry facts that identify it."""

    model: Any
    alias: str
    version: str | None
    location: str


class ModelLoader(Protocol):
    """
    The seam that makes this service testable without MLflow or GCS.

    §15's repository pattern: everything that talks to an external system sits
    behind a thin interface. Unit tests substitute a loader returning a stub
    estimator, so `ModelScoringService` is covered without a tracking server.
    """

    def load(self, alias: str) -> LoadedModel: ...


class MlflowRegistryModelLoader:
    """
    Resolves an alias to a `gs://` path and loads what is there.

    Two steps, not one, and the split matters: the alias → location hop is the
    registry's job and already lives in `ModelRegistryService` (§7), so the
    registry stays the single source of truth for where a model lives. This
    class only knows how to turn a location into an estimator.
    """

    def __init__(self, registry: ModelRegistryService | None = None):
        self._registry = registry or ModelRegistryService()

    def load(self, alias: str) -> LoadedModel:
        location = self._registry.aliased_model_location(alias)
        if location is None:
            raise ModelNotLoadedError(
                f"{config.REGISTERED_MODEL} has no version aliased @{alias}; "
                "promote one with `python -m ml.promote --version N --alias "
                f"{alias}`"
            )
        import mlflow.sklearn

        # sklearn rather than pyfunc: pyfunc's `predict` returns the model's
        # default output, and for a classifier that is a hard 0/1 label. This
        # service has to return a probability — the API's threshold and every
        # drift metric in §12 operate on the score, not the label.
        model = mlflow.sklearn.load_model(location)
        version = self._registry.current_alias_version(alias)
        logger.info("loaded %s @%s (v%s) from %s",
                    config.REGISTERED_MODEL, alias, version, location)
        return LoadedModel(model=model, alias=alias, version=version, location=location)


class ModelScoringService:
    """
    Holds the model and turns request rows into probabilities.

    Loading happens once at startup rather than per request — a GCS download and
    an sklearn deserialise are hundreds of milliseconds, which would dominate
    the latency SLA §9 load-tests against. The consequence is deliberate: moving
    the registry alias does *not* change what a running pod serves. A pod is
    pinned to the version it started with, which is what makes the mesh weight
    the only variable during a §13 ramp; picking up a new version is a rollout.
    """

    def __init__(self, loader: ModelLoader, alias: str, role: str):
        self._loader = loader
        self._alias = alias
        self._role = role
        self._loaded: LoadedModel | None = None

    @property
    def alias(self) -> str:
        return self._alias

    @property
    def role(self) -> str:
        return self._role

    @property
    def version(self) -> str | None:
        return self._loaded.version if self._loaded else None

    @property
    def ready(self) -> bool:
        return self._loaded is not None

    async def load(self) -> None:
        # to_thread because both the GCS read and the deserialise are blocking
        # C-level calls; awaiting them off the loop keeps /healthz answerable
        # while a large model is still loading.
        self._loaded = await asyncio.to_thread(self._loader.load, self._alias)

    async def predict(self, instances: list[list[Any]]) -> list[float]:
        if self._loaded is None:
            raise ModelNotLoadedError("model is not loaded yet")
        return await asyncio.to_thread(self._predict_sync, instances)

    def _predict_sync(self, instances: list[list[Any]]) -> list[float]:
        import pandas as pd

        # The pipeline's first stage is a ColumnTransformer selecting columns by
        # name, so a bare array will not do — the frame has to carry the same
        # column names and dtypes training used. `ModelBuilder.to_matrix` is
        # exactly that coercion, reused rather than reimplemented: a second
        # copy of the dtype rules here is precisely how train/serve skew starts.
        frame = pd.DataFrame(instances, columns=config.MODEL_FEATURES)
        try:
            matrix = ModelBuilder.to_matrix(frame)
            scores = self._loaded.model.predict_proba(matrix)[:, 1]
        except Exception as exc:  # noqa: BLE001 - surfaced as a 400/500 by the caller
            raise ScoringError(f"model failed to score the request: {exc}") from exc
        return [float(score) for score in scores]
