from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.testclient import TestClient

from ml import config
from fraud_prediction_api.main import app
from fraud_prediction_api.services import (
    FeastOnlineFeatureRepository,
    FeatureLookupError,
    ModelServerClient,
    Prediction,
)


PREDICT_URL = "http://model-server/v1/models/fraud-detector:predict"


@pytest.fixture
def client():
    # The real lifespan builds a Feast store whose registry lives in GCS, so it
    # fails here; it logs and leaves `state.features` None rather than raising,
    # which is the unloaded state one of these tests asserts on. The rest
    # substitute a stub, which is the point of the repository seam.
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def ready_client(client):
    client.app.state.features = FeastOnlineFeatureRepository.__new__(
        FeastOnlineFeatureRepository
    )
    return client


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_is_503_without_a_feature_store(client):
    app.state.features = None
    assert client.get("/readyz").status_code == 503


def test_predict_is_503_without_a_feature_store(client):
    """A failed startup must be a 503, not an AttributeError 500."""
    app.state.features = None
    response = client.post(
        "/predict", json={"claim_id": "claim-1", "customer_id": "customer-1"}
    )
    assert response.status_code == 503


def test_prediction_maps_probability_and_threshold(ready_client):
    client = ready_client
    app.state.features.get_features = AsyncMock(
        return_value={name: 1 for name in config.MODEL_FEATURES}
    )
    app.state.model.predict = AsyncMock(
        return_value=Prediction(probability=0.8, model_role="champion", model_version="3")
    )
    response = client.post(
        "/predict",
        json={"claim_id": "claim-1", "customer_id": "customer-1"},
        headers={"x-request-id": "req-1"},
    )
    assert response.status_code == 200
    assert response.json()["fraud_probability"] == 0.8
    assert response.json()["is_fraud"] is True
    assert response.json()["request_id"] == "req-1"


def test_response_reports_the_model_that_answered(ready_client):
    """§13 attribution: the split is invisible to the caller, so the response
    has to carry what the model-server said, not what the API is configured
    with."""
    client = ready_client
    app.state.features.get_features = AsyncMock(
        return_value={name: 1 for name in config.MODEL_FEATURES}
    )
    app.state.model.predict = AsyncMock(
        return_value=Prediction(probability=0.2, model_role="challenger", model_version="7")
    )
    body = client.post(
        "/predict",
        json={"claim_id": "claim-1", "customer_id": "customer-1"},
    ).json()
    assert body["model_role"] == "challenger"
    assert body["model_version"] == "7"


def test_missing_features_becomes_not_found(ready_client):
    client = ready_client
    app.state.features.get_features = AsyncMock(side_effect=FeatureLookupError("missing"))
    response = client.post(
        "/predict",
        json={"claim_id": "claim-1", "customer_id": "customer-1"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_model_server_client_rejects_invalid_probability():
    response = httpx.Response(200, json={"predictions": [1.2]})
    # httpx refuses raise_for_status() on a response with no request attached,
    # so the fake has to carry one.
    response.request = httpx.Request("POST", PREDICT_URL)
    http_client = AsyncMock()
    http_client.post.return_value = response
    client = ModelServerClient("http://model-server", http_client)
    with pytest.raises(Exception):
        await client.predict({name: 1 for name in config.MODEL_FEATURES})


@pytest.mark.asyncio
async def test_model_server_client_reads_attribution_headers():
    response = httpx.Response(
        200,
        json={"predictions": [0.42]},
        headers={"x-model-role": "challenger", "x-model-version": "9"},
    )
    # httpx refuses raise_for_status() on a response with no request attached,
    # so the fake has to carry one.
    response.request = httpx.Request("POST", PREDICT_URL)
    http_client = AsyncMock()
    http_client.post.return_value = response
    client = ModelServerClient("http://model-server", http_client)
    prediction = await client.predict({name: 1 for name in config.MODEL_FEATURES})
    assert prediction == Prediction(probability=0.42, model_role="challenger", model_version="9")


@pytest.mark.asyncio
async def test_model_server_client_falls_back_when_headers_absent():
    """An inference server substituted behind the same Service need not set the
    headers; the configured version is the fallback, never a crash."""
    response = httpx.Response(200, json={"predictions": [0.1]})
    # httpx refuses raise_for_status() on a response with no request attached,
    # so the fake has to carry one.
    response.request = httpx.Request("POST", PREDICT_URL)
    http_client = AsyncMock()
    http_client.post.return_value = response
    client = ModelServerClient("http://model-server", http_client, model_version="production")
    prediction = await client.predict({name: 1 for name in config.MODEL_FEATURES})
    assert prediction.model_role == "unknown"
    assert prediction.model_version == "production"
