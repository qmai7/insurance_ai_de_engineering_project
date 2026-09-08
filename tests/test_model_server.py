"""
model-server unit tests.

Nothing here touches MLflow, GCS or a real estimator: `ModelLoader` is the seam
(§15's repository pattern), so a stub loader is enough to cover load, readiness,
scoring and the request contract.
"""

from __future__ import annotations

import asyncio

import numpy as np
import pytest
from fastapi.testclient import TestClient

from ml import config
from model_server.main import app
from model_server.models import PredictRequest
from model_server.services import (
    LoadedModel,
    ModelNotLoadedError,
    ModelScoringService,
    ScoringError,
)

ROW = [1.0] * len(config.NUMERIC) + [1.0] * len(config.BINARY) + ["a"] * len(config.CATEGORICAL)


class StubEstimator:
    """Returns a fixed second-column probability, shaped like predict_proba."""

    def __init__(self, probability: float = 0.75):
        self._probability = probability

    def predict_proba(self, matrix):
        return np.column_stack([
            np.full(len(matrix), 1 - self._probability),
            np.full(len(matrix), self._probability),
        ])


class StubLoader:
    def __init__(self, model=None, error: Exception | None = None):
        self._model = model if model is not None else StubEstimator()
        self._error = error
        self.calls: list[str] = []

    def load(self, alias: str) -> LoadedModel:
        self.calls.append(alias)
        if self._error:
            raise self._error
        return LoadedModel(model=self._model, alias=alias, version="3", location="gs://b/m")


@pytest.fixture
def scorer():
    return ModelScoringService(loader=StubLoader(), alias="production", role="champion")


# ---------------------------------------------------------------------------
# ModelScoringService
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_not_ready_until_loaded(scorer):
    assert scorer.ready is False
    assert scorer.version is None
    await scorer.load()
    assert scorer.ready is True
    assert scorer.version == "3"


@pytest.mark.asyncio
async def test_predict_before_load_raises(scorer):
    with pytest.raises(ModelNotLoadedError):
        await scorer.predict([ROW])


@pytest.mark.asyncio
async def test_predict_returns_second_column_probability(scorer):
    await scorer.load()
    assert await scorer.predict([ROW]) == [0.75]


@pytest.mark.asyncio
async def test_predict_scores_every_row(scorer):
    """Batch boundary: n rows in, n probabilities out."""
    await scorer.load()
    assert len(await scorer.predict([ROW, ROW, ROW])) == 3


@pytest.mark.asyncio
async def test_estimator_failure_becomes_scoring_error():
    class Exploding:
        def predict_proba(self, matrix):
            raise ValueError("bad dtype")

    service = ModelScoringService(
        loader=StubLoader(model=Exploding()), alias="production", role="champion"
    )
    await service.load()
    with pytest.raises(ScoringError):
        await service.predict([ROW])


@pytest.mark.asyncio
async def test_challenger_role_loads_the_alias_it_was_given():
    loader = StubLoader()
    service = ModelScoringService(loader=loader, alias="challenger", role="challenger")
    await service.load()
    assert loader.calls == ["challenger"]
    assert service.role == "challenger"


# ---------------------------------------------------------------------------
# Request contract — equivalence partitioning on row width (§9)
# ---------------------------------------------------------------------------

def test_request_accepts_exact_feature_width():
    assert PredictRequest(instances=[ROW]).instances == [ROW]


@pytest.mark.parametrize("width_delta", [-1, 1])
def test_request_rejects_off_by_one_width(width_delta):
    width = len(config.MODEL_FEATURES) + width_delta
    with pytest.raises(ValueError):
        PredictRequest(instances=[[1.0] * width])


def test_request_rejects_empty_instances():
    with pytest.raises(ValueError):
        PredictRequest(instances=[])


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------

def _ready_scorer(role: str = "champion") -> ModelScoringService:
    service = ModelScoringService(loader=StubLoader(), alias="production", role=role)
    asyncio.run(service.load())
    return service


@pytest.fixture
def client():
    # The app's real lifespan reaches MLflow and will fail here; it logs and
    # leaves `state.scorer` as None rather than raising, which is exactly the
    # unloaded state two of these tests want to assert on. Tests that need a
    # model substitute one.
    with TestClient(app) as test_client:
        yield test_client


def test_healthz_is_up_regardless_of_model(client):
    """Liveness must not depend on the model — restarting cannot fix a bad
    alias, so a failing liveness probe would only crash-loop the pod."""
    assert client.get("/healthz").json() == {"status": "ok"}


def test_readyz_is_503_before_a_model_is_loaded(client):
    app.state.scorer = None
    assert client.get("/readyz").status_code == 503


def test_readyz_is_503_when_the_scorer_exists_but_has_not_loaded(client):
    app.state.scorer = ModelScoringService(
        loader=StubLoader(), alias="production", role="champion"
    )
    assert client.get("/readyz").status_code == 503


def test_readyz_is_200_once_loaded(client):
    app.state.scorer = _ready_scorer()
    assert client.get("/readyz").status_code == 200


def test_predict_is_503_before_a_model_is_loaded(client):
    """A failed startup must be a 503, not an AttributeError 500."""
    app.state.scorer = None
    response = client.post(
        f"/v1/models/{config.REGISTERED_MODEL}:predict", json={"instances": [ROW]}
    )
    assert response.status_code == 503


def test_predict_sets_attribution_headers(client):
    app.state.scorer = _ready_scorer()
    response = client.post(
        f"/v1/models/{config.REGISTERED_MODEL}:predict", json={"instances": [ROW]}
    )
    assert response.status_code == 200
    assert response.json() == {"predictions": [0.75]}
    assert response.headers["x-model-role"] == "champion"
    assert response.headers["x-model-version"] == "3"


def test_metadata_reports_role_and_version(client):
    app.state.scorer = _ready_scorer(role="challenger")
    body = client.get(f"/v1/models/{config.REGISTERED_MODEL}").json()
    assert body["role"] == "challenger"
    assert body["version"] == "3"
    assert body["ready"] is True


def test_unknown_model_name_is_404(client):
    app.state.scorer = _ready_scorer()
    response = client.post("/v1/models/not-the-model:predict", json={"instances": [ROW]})
    assert response.status_code == 404


def test_malformed_row_width_is_422_not_500(client):
    """Caller error stays a caller error: the width check in PredictRequest is
    what keeps a wrong-width row from becoming a pandas reshape 500."""
    app.state.scorer = _ready_scorer()
    response = client.post(
        f"/v1/models/{config.REGISTERED_MODEL}:predict", json={"instances": [[1.0, 2.0]]}
    )
    assert response.status_code == 422
