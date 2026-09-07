from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.testclient import TestClient

from ml import config
from fraud_prediction_api.main import app
from fraud_prediction_api.services import FeatureLookupError, FeastOnlineFeatureRepository, KServeClient


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_prediction_maps_probability_and_threshold(client):
    app.state.features.get_features = AsyncMock(
        return_value={name: 1 for name in config.MODEL_FEATURES}
    )
    app.state.kserve.predict = AsyncMock(return_value=0.8)
    response = client.post(
        "/predict",
        json={"claim_id": "claim-1", "customer_id": "customer-1"},
        headers={"x-request-id": "req-1"},
    )
    assert response.status_code == 200
    assert response.json()["fraud_probability"] == 0.8
    assert response.json()["is_fraud"] is True
    assert response.json()["request_id"] == "req-1"


def test_missing_features_becomes_not_found(client):
    app.state.features.get_features = AsyncMock(side_effect=FeatureLookupError("missing"))
    response = client.post(
        "/predict",
        json={"claim_id": "claim-1", "customer_id": "customer-1"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_kserve_client_rejects_invalid_probability():
    response = httpx.Response(200, json={"predictions": [1.2]})
    http_client = AsyncMock()
    http_client.post.return_value = response
    client = KServeClient("http://kserve", http_client)
    with pytest.raises(Exception):
        await client.predict({name: 1 for name in config.MODEL_FEATURES})
