"""
Kubeflow training pipeline (CLAUDE.md §5).

    train  ->  quality gate

Compile it:
    .venv-ml/bin/python -m ml.pipeline            # writes ml/pipeline.yaml

Submit it — `ml/submit.py` compiles and submits in one step, so the spec cannot
go stale behind the source:
    kubectl port-forward -n ml-ns svc/ml-pipeline 8888:8888 &
    .venv-ml/bin/python -m ml.submit --wait

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
# postponed evaluation, every annotation is a string, so `summary_root: str` is
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
#
# The run id arrives as its own argument and the path is assembled by the shell
# inside the container, which is uglier than an f-string and is the only thing
# that works. Interpolating the placeholder into a longer string in Python —
#
#     f"{LAKEHOUSE_ROOT}/mlflow/pipelines/{dsl.PIPELINE_JOB_ID_PLACEHOLDER}/..."
#
# — compiles to a constant input value, and KFP substitutes a placeholder only
# where it is the *entire* value. Worse, it fails silently: both steps agree on
# the same unsubstituted path, so the run goes green and the summary is written
# to and read from a literal GCS directory named `{{$.pipeline_job_uuid}}`. The
# only symptom is that every run overwrites the last one, which is exactly the
# property this was meant to provide.
SUMMARY_ROOT = f"{LAKEHOUSE_ROOT}/mlflow/pipelines"


@dsl.container_component
def train_and_register(summary_root: str, run_id: str):
    """Build the training set from Feast, train, evaluate, log and register."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=["sh", "-c", 'python -m ml.train --summary-uri "$0/$1/summary.json"'],
        args=[summary_root, run_id],
    )


@dsl.container_component
def quality_gate(summary_root: str, run_id: str, min_lift: float):
    """Fail the run if the registered version is not a promotion candidate."""
    return dsl.ContainerSpec(
        image=TRAINING_IMAGE,
        command=[
            "sh",
            "-c",
            'python -m ml.gate --summary-uri "$0/$1/summary.json" --min-lift "$2"',
        ],
        args=[summary_root, run_id, min_lift],
    )


@dsl.pipeline(
    name="fraud-model-training",
    description="Feast features -> temporal split -> logistic regression -> MLflow registry -> gate",
)
def fraud_training_pipeline(min_lift: float = 2.0):
    train = train_and_register(
        summary_root=SUMMARY_ROOT, run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER
    )
    train.set_env_variable("MLFLOW_TRACKING_URI", MLFLOW_TRACKING_URI)
    train.set_env_variable("LAKEHOUSE_ROOT", LAKEHOUSE_ROOT)
    train.set_env_variable("FEAST_REPO_PATH", "/feature_store")
    train.set_cpu_request("1").set_cpu_limit("2")
    train.set_memory_request("3Gi").set_memory_limit("4Gi")
    # Retries are off for the same reason the Job sets backoffLimit 0: this step
    # fails on a broken point-in-time join or an unreachable store, and neither
    # improves on a second attempt.
    train.set_retry(0)

    gate = quality_gate(
        summary_root=SUMMARY_ROOT,
        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
        min_lift=min_lift,
    )
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
