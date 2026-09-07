from __future__ import annotations

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
    KServeClient,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    http_client = httpx.AsyncClient(timeout=float(os.getenv("KSERVE_TIMEOUT_SECONDS", "5")))
    app.state.features = FeastOnlineFeatureRepository(FeatureStore(repo_path=config.FEAST_REPO_PATH))
    app.state.kserve = KServeClient(
        url=os.getenv(
            "KSERVE_PREDICT_URL",
            "http://fraud-detector-predictor.kserve-ns.svc.cluster.local/v1/models/fraud-detector:predict",
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


@app.get("/readyz", response_model=HealthResponse)
async def readyz(request: Request) -> HealthResponse:
    if not hasattr(request.app.state, "features"):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="not ready")
    return HealthResponse(status="ready")


@app.post("/predict", response_model=PredictionResponse)
async def predict(payload: PredictionRequest, request: Request) -> PredictionResponse:
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    try:
        features = await request.app.state.features.get_features(
            payload.claim_id,
            payload.customer_id,
        )
        probability = await request.app.state.kserve.predict(features)
    except FeatureLookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except InferenceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    threshold = request.app.state.threshold
    return PredictionResponse(
        claim_id=payload.claim_id,
        customer_id=payload.customer_id,
        fraud_probability=probability,
        is_fraud=probability >= threshold,
        threshold=threshold,
        model_name=request.app.state.kserve.model_name,
        model_version=request.app.state.kserve.model_version,
        request_id=request_id,
    )
