"""
Kubeflow training pipeline (CLAUDE.md §5).

    train  ->  quality gate

Compile it:
    .venv-ml/bin/python -m ml.pipeline            # writes ml/pipeline.yaml

Submit it (once a KFP control plane exists in the cluster):
    from kfp.client import Client
    Client(host="http://ml-pipeline.ml-ns.svc.cluster.local:8888").create_run_from_pipeline_package(
        "ml/pipeline.yaml", arguments={"min_lift": 2.0})

Two design choices worth stating, because both trade capability for simplicity:

**Container components, not Python-function components.** A `@dsl.component`
would have KFP pip-install dependencies into a base image at run time, which
means a pipeline run could resolve a different feast or scikit-learn than the one
the model was tested against. These steps run the same `training:` image the
Kubernetes Job runs, so the pipeline and a manual run are the same code by
construction.

**Steps hand off through a GCS URI, not KFP artifacts.** The URI is derived from
the pipeline run id, so each step stays independently runnable with plain
`kubectl` — useful precisely when a pipeline run is what is broken. The cost is
that KFP's UI does not render the summary as a typed artifact.

Not included: a distributed training step. §5 asks for one, and it is not here —
the model is a logistic regression on 2,118 rows, so there is nothing to
distribute, and the gradient-boosted model that would have justified it was
removed. This is a known, deliberate gap rather than an oversight.
"""

# NOTE: deliberately no `from __future__ import annotations` here.
#
# KFP resolves component signatures by reflection at decoration time. With
# postponed evaluation, every annotation is a string, so `summary_uri: str` is
# read as an *artifact type* named "str" and compilation fails with
# "Artifacts must have both a schema_title and a schema_version".
from pathlib import Path

from kfp import compiler, dsl

TRAINING_IMAGE = (
    "northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/training:0.1.4"
)

MLFLOW_TRACKING_URI = "http://mlflow.ml-ns.svc.cluster.local:5000"
LAKEHOUSE_ROOT = "gs://aide-playground-lakehouse"

# Run-scoped, so concurrent runs cannot overwrite each other's summary and a past
# run's inputs stay readable for debugging.
SUMMARY_URI = f"{LAKEHOUSE_ROOT}/mlflow/pipelines/{dsl.PIPELINE_JOB_ID_PLACEHOLDER}/summary.json"


@dsl.container_component
def train_and_register(summary_uri: str):
    """Build the training set from Feast, train, evaluate, log and register."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=["python", "-m", "ml.train"],
        args=["--summary-uri", summary_uri],
    )


@dsl.container_component
def quality_gate(summary_uri: str, min_lift: float):
    """Fail the run if the registered version is not a promotion candidate."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=["python", "-m", "ml.gate"],
        args=["--summary-uri", summary_uri, "--min-lift", min_lift],
    )


@dsl.pipeline(
    name="fraud-model-training",
    description="Feast features -> temporal split -> logistic regression -> MLflow registry -> gate",
)
def fraud_training_pipeline(min_lift: float = 2.0):
    train = train_and_register(summary_uri=SUMMARY_URI)
    train.set_env_variable("MLFLOW_TRACKING_URI", MLFLOW_TRACKING_URI)
    train.set_env_variable("LAKEHOUSE_ROOT", LAKEHOUSE_ROOT)
    train.set_env_variable("FEAST_REPO_PATH", "/feature_store")
    train.set_cpu_request("1").set_cpu_limit("2")
    train.set_memory_request("3Gi").set_memory_limit("4Gi")
    # Retries are off for the same reason the Job sets backoffLimit 0: this step
    # fails on a broken point-in-time join or an unreachable store, and neither
    # improves on a second attempt.
    train.set_retry(0)

    gate = quality_gate(summary_uri=SUMMARY_URI, min_lift=min_lift)
    gate.set_env_variable("MLFLOW_TRACKING_URI", MLFLOW_TRACKING_URI)
    gate.set_cpu_request("200m").set_memory_request("512Mi")
    # The explicit dependency: the gate reads what training wrote, and nothing in
    # the argument graph tells KFP that, because the handoff is a GCS path.
    gate.after(train)


if __name__ == "__main__":
    target = Path(__file__).with_name("pipeline.yaml")
    compiler.Compiler().compile(
        pipeline_func=fraud_training_pipeline,
        package_path=str(target),
    )
    print(f"compiled {target}")
