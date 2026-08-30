"""
Training entrypoint — the productionised form of `notebooks/01_fraud_model_baseline.ipynb`.

Wiring only. Every step is a service from `services.py` reading through a
repository from `repositories.py`, so this file is the assembly and contains no
logic worth unit-testing on its own. That is the point: §5's Kubeflow pipeline
replaces *this file* with a graph of components calling the same services, and
nothing about the model changes when it does.

Run in-cluster:
    kubectl apply -f ml/training-job.yaml

Run locally against a port-forwarded MLflow:
    kubectl port-forward -n ml-ns svc/mlflow 5000:5000
    MLFLOW_TRACKING_URI=http://localhost:5000 FEAST_REPO_PATH=feature_store \
      .venv-ml/bin/python -m ml.train
"""

from __future__ import annotations

import argparse
import json
import sys

from . import config
from .repositories import (
    DeltaLogDataVersionRepository,
    FeastFeatureRepository,
    ParquetClaimSpineRepository,
    ParquetLabelRepository,
)
from .services import (
    EvaluationService,
    ModelBuilder,
    ModelRegistryService,
    SplitService,
    TrainingDataService,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    print(f"tracking uri : {config.TRACKING_URI}")
    print(f"experiment   : {config.EXPERIMENT}")
    print(f"feast repo   : {config.FEAST_REPO_PATH}")

    features = FeastFeatureRepository()
    data_service = TrainingDataService(
        spine_repository=ParquetClaimSpineRepository(),
        label_repository=ParquetLabelRepository(),
        feature_repository=features,
        version_repository=DeltaLogDataVersionRepository(),
    )

    print("\n1. building training dataset")
    dataset = data_service.build()
    print(f"   {dataset.rows} rows, {len(dataset.feature_refs)} features, "
          f"data_version={dataset.data_version}")

    print("\n2. splitting by time")
    split = SplitService().split(dataset)
    for key, value in split.describe().items():
        print(f"   {key}: {value}")

    print("\n3. training")
    builder = ModelBuilder()
    X_train = builder.to_matrix(split.train)
    y_train = split.train[config.LABEL].to_numpy()
    model = builder.build()
    model.fit(X_train, y_train)
    print(f"   fitted on {len(X_train)} rows, {int(y_train.sum())} positives")

    print("\n4. evaluating")
    X_val = builder.to_matrix(split.validation)
    y_val = split.validation[config.LABEL].to_numpy()
    evaluation = EvaluationService().evaluate(model, X_val, y_val)
    print(f"   PR-AUC {evaluation.pr_auc:.3f} (random {evaluation.baseline_pr_auc:.3f})"
          f"  ROC-AUC {evaluation.roc_auc:.3f}")
    for point in evaluation.budget_curve:
        print(f"   budget {point['budget']:.0%}: precision {point['precision']:.3f} "
              f"recall {point['recall']:.3f} lift {point['lift_vs_random']:.1f}x")

    if args.no_log:
        print("\n--no-log: skipping MLflow, nothing registered")
        return 0

    print("\n5. logging to MLflow and registering")
    registry = ModelRegistryService()
    result = registry.log_run(
        model=model,
        dataset=dataset,
        split=split,
        evaluation=evaluation,
        params=builder.params(),
        # A real row, so the logged signature is inferred from data the API will
        # actually send rather than from a hand-written schema.
        input_example=X_train.head(5),
        extra_tags={"feast_registry": features.describe()["registry"]},
    )
    print(f"   run_id       : {result['run_id']}")
    print(f"   model_uri    : {result['model_uri']}")
    print(f"   registered   : {config.REGISTERED_MODEL} v{result['version']}")

    live = registry.current_alias_version()
    print(f"\n   '{config.PRODUCTION_ALIAS}' alias currently points at: {live or 'nothing'}")
    if live != result["version"]:
        # Promotion is a separate, deliberate act — see ModelRegistryService.
        print(f"   to promote this version:\n"
              f"     python -m ml.promote --version {result['version']}")

    summary = {
        **result,
        "data_version": dataset.data_version,
        "pr_auc": evaluation.pr_auc,
        "roc_auc": evaluation.roc_auc,
        "baseline_pr_auc": evaluation.baseline_pr_auc,
        "registered_model": config.REGISTERED_MODEL,
    }
    print("\n" + json.dumps(summary))

    if args.summary_uri:
        # Written for the next pipeline step to read. A URI rather than KFP's
        # artifact plumbing: the steps already share a bucket, so passing a path
        # keeps each one independently runnable with plain kubectl.
        _write_summary(args.summary_uri, summary)
        print(f"   summary written to {args.summary_uri}")

    return 0


def _write_summary(uri: str, summary: dict) -> None:
    payload = json.dumps(summary, indent=2)
    if uri.startswith("gs://"):
        import gcsfs

        with gcsfs.GCSFileSystem().open(uri, "w") as handle:
            handle.write(payload)
    else:
        from pathlib import Path

        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and register the fraud model.")
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="train and evaluate but do not touch MLflow (for verifying data access).",
    )
    parser.add_argument(
        "--summary-uri",
        default=None,
        help="write the run summary (metrics, version, data_version) here as JSON. "
             "Accepts a gs:// URI or a local path.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
