from __future__ import annotations

from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
    claim_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)


class PredictionResponse(BaseModel):
    claim_id: str
    customer_id: str
    fraud_probability: float = Field(ge=0.0, le=1.0)
    is_fraud: bool
    threshold: float = Field(ge=0.0, le=1.0)
    model_name: str
    # Which side of the §13 split answered, reported by the model-server rather
    # than assumed by the API.
    model_role: str
    model_version: str
    request_id: str


class HealthResponse(BaseModel):
    status: str
