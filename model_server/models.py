from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from ml import config


class PredictRequest(BaseModel):
    """
    The KServe v1 request shape, kept deliberately.

    KServe is gone (see docs/api.md), but its wire protocol is a published,
    widely-implemented contract: `instances` is a list of rows, each row a list
    of feature values in a fixed order. Inventing a bespoke payload would buy
    nothing and would make swapping in a real inference server later — Triton,
    MLServer, TorchServe, all of which speak this — a client rewrite.
    """

    instances: list[list[Any]] = Field(min_length=1)

    @field_validator("instances")
    @classmethod
    def _rows_match_the_feature_contract(cls, rows: list[list[Any]]) -> list[list[Any]]:
        # Rejected here rather than inside the sklearn pipeline: a wrong-width
        # row would otherwise surface as a pandas reshape error deep in the
        # ColumnTransformer, i.e. a 500 for what is a caller mistake. The
        # boundary condition worth testing is exactly len(MODEL_FEATURES) ± 1.
        width = len(config.MODEL_FEATURES)
        for index, row in enumerate(rows):
            if len(row) != width:
                raise ValueError(
                    f"instance {index} has {len(row)} values, expected {width} "
                    f"(one per entry of ml.config.MODEL_FEATURES)"
                )
        return rows


class PredictResponse(BaseModel):
    """`predictions[i]` is P(fraud) for `instances[i]` — a probability, not a label.

    Thresholding is the API's job, not the model's: the review budget in §4 is a
    business decision that changes without retraining.
    """

    predictions: list[float]


class ModelMetadata(BaseModel):
    """
    What `/v1/models/{name}` answers — and the whole basis of §13 attribution.

    Without this, a champion/challenger comparison cannot say *which* model
    produced a prediction: the mesh split is invisible to the caller, and both
    roles answer on the same Service name. The API copies `role` and `version`
    onto every prediction response so Grafana can group by them.
    """

    name: str
    alias: str
    role: str
    version: str | None
    ready: bool
