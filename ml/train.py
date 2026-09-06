"""
Training entrypoint — the productionised form of `notebooks/01_fraud_model_baseline.ipynb`.

Supports step-wise execution for Kubeflow pipeline granularity:
  --step build_dataset      Build training dataset from Feast
  --step split_by_time      Temporal train/validation split
  --step train              Train the model
  --step evaluate           Evaluate on validation set
  --step log_to_mlflow      Log run and register model
  
Or run all steps at once (default, --step=all).
"""

from __future__ import annotations

import argparse
import json
import joblib
import sys
from pathlib import Path

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
    TrainingDataset,
    DataSplit,
    Evaluation,
)


def _build_dataset(features: FeastFeatureRepository) -> TrainingDataset:
    """Step 1: Build training dataset from Feast and labels."""
    print("\n1. building training dataset")
    data_service = TrainingDataService(
        spine_repository=ParquetClaimSpineRepository(),
        label_repository=ParquetLabelRepository(),
        feature_repository=features,
        version_repository=DeltaLogDataVersionRepository(),
    )
    dataset = data_service.build()
    print(f"   {dataset.rows} rows, {len(dataset.feature_refs)} features, "
          f"data_version={dataset.data_version}")
    return dataset


def _split_dataset(dataset: TrainingDataset) -> DataSplit:
    """Step 2: Temporal train/validation split."""
    print("\n2. splitting by time")
    split = SplitService().split(dataset)
    for key, value in split.describe().items():
        print(f"   {key}: {value}")
    return split


def _train_model(split: DataSplit) -> tuple:
    """Step 3: Train the logistic regression model."""
    print("\n3. training")
    builder = ModelBuilder()
    X_train = builder.to_matrix(split.train)
    y_train = split.train[config.LABEL].to_numpy()
    model = builder.build()
    model.fit(X_train, y_train)
    print(f"   fitted on {len(X_train)} rows, {int(y_train.sum())} positives")
    return model, builder, X_train


def _evaluate_model(model, split: DataSplit) -> Evaluation:
    """Step 4: Evaluate model on validation set."""
    print("\n4. evaluating")
    builder = ModelBuilder()
    X_val = builder.to_matrix(split.validation)
    y_val = split.validation[config.LABEL].to_numpy()
    evaluation = EvaluationService().evaluate(model, X_val, y_val)
    print(f"   PR-AUC {evaluation.pr_auc:.3f} (random {evaluation.baseline_pr_auc:.3f})"
          f"  ROC-AUC {evaluation.roc_auc:.3f}")
    for point in evaluation.budget_curve:
        print(f"   budget {point['budget']:.0%}: precision {point['precision']:.3f} "
              f"recall {point['recall']:.3f} lift {point['lift_vs_random']:.1f}x")
    return evaluation


def _log_to_mlflow(model, dataset: TrainingDataset, split: DataSplit, 
                   evaluation: Evaluation, X_train, features: FeastFeatureRepository,
                   builder: ModelBuilder) -> dict:
    """Step 5: Log to MLflow and register model."""
    print("\n5. logging to MLflow and registering")
    registry = ModelRegistryService()
    result = registry.log_run(
        model=model,
        dataset=dataset,
        split=split,
        evaluation=evaluation,
        params=builder.params(),
        input_example=X_train.head(5),
        extra_tags={"feast_registry": features.describe()["registry"]},
    )
    print(f"   run_id       : {result['run_id']}")
    print(f"   model_uri    : {result['model_uri']}")
    print(f"   registered   : {config.REGISTERED_MODEL} v{result['version']}")
    
    live = registry.current_alias_version()
    print(f"\n   '{config.PRODUCTION_ALIAS}' alias currently points at: {live or 'nothing'}")
    if live != result["version"]:
        print(f"   to promote this version:\n"
              f"     python -m ml.promote --version {result['version']}")
    
    return result


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    print(f"tracking uri : {config.TRACKING_URI}")
    print(f"experiment   : {config.EXPERIMENT}")
    print(f"feast repo   : {config.FEAST_REPO_PATH}")

    features = FeastFeatureRepository()
    builder = ModelBuilder()
    model = None
    X_train = None
    dataset = None
    split = None
    evaluation = None

    # Step 1: Build dataset
    if args.step in ("all", "build_dataset"):
        dataset = _build_dataset(features)
        if args.step == "build_dataset":
            _save_artifact(args.dataset_uri, dataset)
            return 0
    elif args.step in ("split_by_time", "evaluate", "log_to_mlflow"):
        # Need dataset for downstream steps
        if args.dataset_uri:
            dataset = _load_artifact(args.dataset_uri)

    # Step 2: Split by time
    if args.step in ("all", "split_by_time"):
        split = _split_dataset(dataset)
        if args.step == "split_by_time":
            _save_artifact(args.split_uri, split)
            return 0
    elif args.step in ("train", "evaluate", "log_to_mlflow"):
        # Need split for downstream steps
        if args.split_uri:
            split = _load_artifact(args.split_uri)

    # Step 3: Train model
    if args.step in ("all", "train"):
        model, builder, X_train = _train_model(split)
        if args.step == "train":
            _save_model(args.model_uri, model)
            return 0
    elif args.step in ("evaluate", "log_to_mlflow"):
        # Need model for downstream steps
        if args.model_uri:
            model = _load_model(args.model_uri)
        # If log_to_mlflow, we'll need to reconstruct X_train from split
        if args.step == "log_to_mlflow" and split:
            X_train = builder.to_matrix(split.train)

    # Step 4: Evaluate
    if args.step in ("all", "evaluate"):
        evaluation = _evaluate_model(model, split)
        if args.step == "evaluate":
            _save_artifact(args.evaluation_uri, evaluation)
            return 0
    elif args.step == "log_to_mlflow":
        # Need evaluation for final step
        if args.evaluation_uri:
            evaluation = _load_artifact(args.evaluation_uri)

    # Step 5: Log to MLflow (only if not --no-log)
    if args.no_log:
        print("\n--no-log: skipping MLflow, nothing registered")
        return 0

    if args.step not in ("all", "log_to_mlflow"):
        # For non-log steps in sequence, return here
        return 0

    # Only log_to_mlflow or all reaches here
    if not model or not dataset or not split or not evaluation:
        raise ValueError(
            f"--step log_to_mlflow requires: model, dataset, split, evaluation. "
            f"Got: model={model is not None}, dataset={dataset is not None}, "
            f"split={split is not None}, evaluation={evaluation is not None}"
        )

    result = _log_to_mlflow(model, dataset, split, evaluation, X_train, features, builder)

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
        _write_summary(args.summary_uri, summary)
        print(f"   summary written to {args.summary_uri}")

    return 0


def _save_artifact(uri: str, obj) -> None:
    """Serialize object to GCS or local path."""
    if not uri:
        return
    import io
    buffer = io.BytesIO()
    joblib.dump(obj, buffer)
    payload = buffer.getvalue()
    if uri.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(uri, "wb") as handle:
            handle.write(payload)
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(payload)
    print(f"   artifact written to {uri}")


def _load_artifact(uri: str):
    """Deserialize object from GCS or local path."""
    if not uri:
        raise ValueError(f"artifact URI is required but not provided")
    import io
    if uri.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(uri, "rb") as handle:
            return joblib.load(io.BytesIO(handle.read()))
    else:
        return joblib.load(uri)


def _save_model(uri: str, model) -> None:
    """Save scikit-learn model to GCS or local path."""
    if not uri:
        return
    if uri.startswith("gs://"):
        import gcsfs
        import io
        buffer = io.BytesIO()
        joblib.dump(model, buffer)
        with gcsfs.GCSFileSystem().open(uri, "wb") as handle:
            handle.write(buffer.getvalue())
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, uri)
    print(f"   model saved to {uri}")


def _load_model(uri: str):
    """Load scikit-learn model from GCS or local path."""
    import io
    if uri.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(uri, "rb") as handle:
            return joblib.load(io.BytesIO(handle.read()))
    else:
        return joblib.load(uri)


def _write_summary(uri: str, summary: dict) -> None:
    """Write JSON summary for quality gate."""
    payload = json.dumps(summary, indent=2)
    if uri.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(uri, "w") as handle:
            handle.write(payload)
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_text(payload)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train fraud model with step-wise Kubeflow execution.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Step-wise execution:
  --step build_dataset      Build training dataset from Feast
  --step split_by_time      Temporal train/validation split
  --step train              Train the model
  --step evaluate           Evaluate on validation set
  --step log_to_mlflow      Log run and register model
  
  Default: all (run all steps in sequence)
        """)
    parser.add_argument(
        "--step",
        default="all",
        choices=["all", "build_dataset", "split_by_time", "train", "evaluate", "log_to_mlflow"],
        help="which step(s) to run (default: all)",
    )
    parser.add_argument(
        "--dataset-uri",
        default=None,
        help="GCS or local path to persisted TrainingDataset (for --step != build_dataset)",
    )
    parser.add_argument(
        "--split-uri",
        default=None,
        help="GCS or local path to persisted DataSplit (for --step != split_by_time)",
    )
    parser.add_argument(
        "--model-uri",
        default=None,
        help="GCS or local path to persisted model .joblib (for --step != train)",
    )
    parser.add_argument(
        "--evaluation-uri",
        default=None,
        help="GCS or local path to persisted Evaluation (for --step != evaluate)",
    )
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
