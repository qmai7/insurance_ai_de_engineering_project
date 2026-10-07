from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from feast import FeatureStore

from ml import config
from fraud_prediction_api.models import HealthResponse, PredictionRequest, PredictionResponse
from fraud_prediction_api.services import (
    FeastOnlineFeatureRepository,
    FeatureLookupError,
    InferenceError,
    ModelServerClient,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    http_client = httpx.AsyncClient(timeout=float(os.getenv("MODEL_SERVER_TIMEOUT_SECONDS", "5")))

    # Feast's registry lives in GCS, so constructing the store is a network
    # call. Set to None first and tolerate the failure: a pod that starts,
    # fails /readyz and logs why is diagnosable and stays out of the Service's
    # endpoints, whereas raising here crash-loops on a transient GCS blip and
    # tells you nothing beyond "it restarted".
    app.state.features = None
    try:
        app.state.features = FeastOnlineFeatureRepository(
            FeatureStore(repo_path=config.FEAST_REPO_PATH)
        )
    except Exception:
        logger.exception("Feast store init failed; API will stay not-ready")

    # One Service name for both champion and challenger. The API is not told
    # which model it is calling and does not choose — the mesh decides on a
    # per-request basis, and the answer comes back on the response headers.
    app.state.model = ModelServerClient(
        url=os.getenv(
            "MODEL_SERVER_PREDICT_URL",
            "http://model-server.api-serving-ns.svc.cluster.local:8080"
            "/v1/models/fraud-detector:predict",
        ),
        client=http_client,
        model_name=os.getenv("MODEL_NAME", config.REGISTERED_MODEL),
        model_version=os.getenv("MODEL_VERSION", config.PRODUCTION_ALIAS),
    )
    app.state.threshold = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
    yield
    await http_client.aclose()


app = FastAPI(title="fraud-prediction-api", lifespan=lifespan)


@app.get("/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return HealthResponse(status="ok")


def _features(request: Request):
    """One definition of "no feature store" for every handler — otherwise a
    failed startup surfaces as an AttributeError 500 rather than a 503."""
    features = getattr(request.app.state, "features", None)
    if features is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="not ready"
        )
    return features


@app.get("/readyz", response_model=HealthResponse)
async def readyz(request: Request) -> HealthResponse:
    _features(request)
    return HealthResponse(status="ready")


@app.post("/predict", response_model=PredictionResponse)
async def predict(payload: PredictionRequest, request: Request) -> PredictionResponse:
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    try:
        features = await _features(request).get_features(
            payload.claim_id,
            payload.customer_id,
        )
        prediction = await request.app.state.model.predict(features)
    except FeatureLookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except InferenceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    threshold = request.app.state.threshold
    return PredictionResponse(
        claim_id=payload.claim_id,
        customer_id=payload.customer_id,
        fraud_probability=prediction.probability,
        is_fraud=prediction.probability >= threshold,
        threshold=threshold,
        model_name=request.app.state.model.model_name,
        # Echoed from the model-server, not from this pod's config: under a
        # §13 traffic split these are the only record of which model scored
        # this claim, and every A/B proxy metric groups by them.
        model_role=prediction.model_role,
        model_version=prediction.model_version,
        request_id=request_id,
    )
