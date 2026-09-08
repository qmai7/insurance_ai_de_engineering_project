from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, status

from ml import config
from model_server.models import ModelMetadata, PredictRequest, PredictResponse
from model_server.services import (
    MlflowRegistryModelLoader,
    ModelNotLoadedError,
    ModelScoringService,
    ScoringError,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

MODEL_NAME = os.getenv("MODEL_NAME", config.REGISTERED_MODEL)

# `role` is what §13 groups by; `alias` is what this pod loads. They are separate
# knobs so a champion/challenger pair can be run off two *versions of the same
# alias* during a rollback drill, or two different aliases in the normal case.
MODEL_ROLE = os.getenv("MODEL_ROLE", "champion")
MODEL_ALIAS = os.getenv(
    "MODEL_ALIAS",
    config.CHALLENGER_ALIAS if MODEL_ROLE == "challenger" else config.PRODUCTION_ALIAS,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # `scorer` is set to None first so every handler has one definition of "no
    # model": the construction below reaches MLflow, and a handler that assumed
    # the attribute exists would turn a failed startup into an AttributeError
    # 500 instead of the 503 it is.
    app.state.scorer = None
    try:
        scorer = ModelScoringService(
            loader=MlflowRegistryModelLoader(),
            alias=MODEL_ALIAS,
            role=MODEL_ROLE,
        )
        await scorer.load()
        app.state.scorer = scorer
    except Exception:
        # Deliberately not fatal. A crash-looping pod tells you nothing beyond
        # "it restarted", whereas a pod that starts, fails /readyz and logs the
        # reason is diagnosable — and, on the challenger path, an unpromoted
        # alias is an expected state, not an outage. /readyz keeps it out of the
        # Service's endpoints either way, so it never receives traffic.
        logger.exception("model load failed; server will stay not-ready")
    yield


app = FastAPI(title="model-server", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up. Says nothing about the model, on purpose —
    a liveness probe that fails on an unloaded model would restart a pod that
    restarting cannot fix."""
    return {"status": "ok"}


def _scorer(request: Request) -> ModelScoringService:
    scorer = getattr(request.app.state, "scorer", None)
    if scorer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="model not loaded",
        )
    return scorer


@app.get("/readyz")
async def readyz(request: Request) -> dict[str, str]:
    if not _scorer(request).ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="model not loaded",
        )
    return {"status": "ready"}


@app.get("/v1/models/{name}", response_model=ModelMetadata)
async def metadata(name: str, request: Request) -> ModelMetadata:
    if name != MODEL_NAME:
        raise HTTPException(status_code=404, detail=f"no model named {name!r}")
    scorer = _scorer(request)
    return ModelMetadata(
        name=MODEL_NAME,
        alias=scorer.alias,
        role=scorer.role,
        version=scorer.version,
        ready=scorer.ready,
    )


@app.post("/v1/models/{name}:predict", response_model=PredictResponse)
async def predict(name: str, payload: PredictRequest, request: Request, response: Response):
    if name != MODEL_NAME:
        raise HTTPException(status_code=404, detail=f"no model named {name!r}")
    scorer = _scorer(request)
    try:
        predictions = await scorer.predict(payload.instances)
    except ModelNotLoadedError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ScoringError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Which model answered, on the response itself. The mesh split is invisible
    # to the caller — both roles answer on one Service name — so without these
    # headers the API cannot attribute a prediction to a version, and §13's
    # disagreement rate and per-version metrics have nothing to group by.
    response.headers["x-model-role"] = scorer.role
    response.headers["x-model-version"] = scorer.version or "unknown"
    return PredictResponse(predictions=predictions)
