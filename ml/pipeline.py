"""
Kubeflow training pipeline with granular steps for easier debugging and management.

Pipeline flow:

    build_training_dataset
          |
    split_by_time
          |
    train_model
          |
    evaluate_model
          |
    log_to_mlflow
          |
    quality_gate

Each step is independently runnable and writes artifacts to GCS for the next step to read.
This design enables:
  - Debugging individual steps without re-running previous steps
  - Restarting from any step if an intermediate step fails
  - Understanding exactly where failures occur

Compile:
    .venv-ml/bin/python -m ml.pipeline

Submit:
    .venv-ml/bin/python -m ml.submit --wait
"""

from pathlib import Path

from kfp import compiler, dsl

TRAINING_IMAGE = (
    "northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/training:0.1.5"
)

MLFLOW_TRACKING_URI = "http://mlflow.ml-ns.svc.cluster.local:5000"
LAKEHOUSE_ROOT = "gs://aide-playground-lakehouse"
PIPELINE_ROOT = f"{LAKEHOUSE_ROOT}/mlflow/pipelines"


@dsl.container_component
def build_training_dataset(run_id: str, pipeline_root: str):
    """Step 1: Build training dataset from Feast features and labels."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.train --step build_dataset --dataset-uri "$0/$1/dataset.joblib"'
        ],
        args=[pipeline_root, run_id],
    )


@dsl.container_component
def split_by_time(run_id: str, pipeline_root: str):
    """Step 2: Temporal train/validation split."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.train --step split_by_time '
            '--dataset-uri "$0/$1/dataset.joblib" '
            '--split-uri "$0/$1/split.joblib"'
        ],
        args=[pipeline_root, run_id],
    )


@dsl.container_component
def train_model(run_id: str, pipeline_root: str):
    """Step 3: Train logistic regression model."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.train --step train '
            '--split-uri "$0/$1/split.joblib" '
            '--model-uri "$0/$1/model.joblib"'
        ],
        args=[pipeline_root, run_id],
    )


@dsl.container_component
def evaluate_model(run_id: str, pipeline_root: str):
    """Step 4: Evaluate model on validation set."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.train --step evaluate '
            '--split-uri "$0/$1/split.joblib" '
            '--model-uri "$0/$1/model.joblib" '
            '--evaluation-uri "$0/$1/evaluation.joblib"'
        ],
        args=[pipeline_root, run_id],
    )


@dsl.container_component
def log_to_mlflow(run_id: str, pipeline_root: str):
    """Step 5: Log run to MLflow and register model."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.train --step log_to_mlflow '
            '--dataset-uri "$0/$1/dataset.joblib" '
            '--split-uri "$0/$1/split.joblib" '
            '--model-uri "$0/$1/model.joblib" '
            '--evaluation-uri "$0/$1/evaluation.joblib" '
            '--summary-uri "$0/$1/summary.json"'
        ],
        args=[pipeline_root, run_id],
    )


@dsl.container_component
def quality_gate(summary_root: str, run_id: str, min_lift: float):
    """Step 6: Quality gate — fail if model does not meet threshold."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh", "-c",
            'python -m ml.gate --summary-uri "$0/$1/summary.json" --min-lift "$2"',
        ],
        args=[summary_root, run_id, min_lift],
    )


@dsl.pipeline(
    name="fraud-model-training",
    description="Granular training pipeline: dataset → split → train → evaluate → log → gate",
)
def fraud_training_pipeline(min_lift: float = 2.0):
    """Define the Kubeflow DAG with explicit dependencies."""
    
    # Step 1: Build dataset
    build_ds = build_training_dataset(
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        pipeline_root=PIPELINE_ROOT,
    )
    build_ds.set_env_variable("LAKEHOUSE_ROOT", LAKEHOUSE_ROOT)
    build_ds.set_env_variable("FEAST_REPO_PATH", "/feature_store")
    build_ds.set_cpu_request("1").set_cpu_limit("2")
    build_ds.set_memory_request("3Gi").set_memory_limit("4Gi")
    build_ds.set_retry(0)
    
    # Step 2: Split by time
    split = split_by_time(
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        pipeline_root=PIPELINE_ROOT,
    )
    split.set_cpu_request("1").set_cpu_limit("2")
    split.set_memory_request("3Gi").set_memory_limit("4Gi")
    split.set_retry(0)
    split.after(build_ds)
    
    # Step 3: Train model
    train = train_model(
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        pipeline_root=PIPELINE_ROOT,
    )
    train.set_cpu_request("1").set_cpu_limit("2")
    train.set_memory_request("3Gi").set_memory_limit("4Gi")
    train.set_retry(0)
    train.after(split)
    
    # Step 4: Evaluate
    evaluate = evaluate_model(
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        pipeline_root=PIPELINE_ROOT,
    )
    evaluate.set_cpu_request("500m").set_cpu_limit("1")
    evaluate.set_memory_request("2Gi").set_memory_limit("2Gi")
    evaluate.set_retry(0)
    evaluate.after(train)
    
    # Step 5: Log to MLflow
    log = log_to_mlflow(
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        pipeline_root=PIPELINE_ROOT,
    )
    log.set_env_variable("MLFLOW_TRACKING_URI", MLFLOW_TRACKING_URI)
    log.set_cpu_request("500m").set_cpu_limit("1")
    log.set_memory_request("2Gi").set_memory_limit("2Gi")
    log.set_retry(0)
    log.after(evaluate)
    
    # Step 6: Quality gate
    gate = quality_gate(
        summary_root=PIPELINE_ROOT,
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        min_lift=min_lift,
    )
    gate.set_env_variable("MLFLOW_TRACKING_URI", MLFLOW_TRACKING_URI)
    gate.set_cpu_request("200m").set_memory_request("512Mi")
    gate.set_retry(0)
    gate.after(log)


if __name__ == "__main__":
    target = Path(__file__).with_name("pipeline.yaml")
    compiler.Compiler().compile(
        pipeline_func=fraud_training_pipeline,
        package_path=str(target),
    )
    print(f"compiled {target}")
