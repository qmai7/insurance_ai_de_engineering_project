"""
Compile and submit the Kubeflow training pipeline.

    kubectl port-forward -n ml-ns svc/ml-pipeline 8888:8888 &
    .venv-ml/bin/python -m ml.submit --wait

In short, this script automates this sequence
    -> compile to pipeline.yaml
    -> connect to Kubeflow
    -> find/create experiment
    -> submit run
    -> optionally wait for success/failure
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE = Path(__file__).with_name("pipeline.yaml")
DEFAULT_HOST = "http://localhost:8888"
EXPERIMENT = "insurance-fraud"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Submit the fraud training pipeline to KFP.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"KFP API (default: {DEFAULT_HOST})")
    parser.add_argument("--min-lift", type=float, default=2.0, help="quality gate threshold")
    parser.add_argument(
        "--experiment",
        default=EXPERIMENT,
        help=f"KFP experiment to group the run under (default: {EXPERIMENT})",
    )
    parser.add_argument(
        "--no-compile",
        action="store_true",
        help="submit ml/pipeline.yaml as-is instead of recompiling ml/pipeline.py first",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="block until the run finishes and exit non-zero if it did not succeed",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="seconds to wait with --wait (default: 3600)",
    )
    args = parser.parse_args(argv)

    # Recompile by default. The compiled YAML is committed so the pipeline is
    # reviewable in a diff, which makes it just as easy for it to fall behind the
    # source it was generated from — and a run of a stale spec is a confusing
    # thing to debug, because the code on disk is right.
    if not args.no_compile:
        from ml import pipeline as pipeline_module
        from kfp import compiler

        compiler.Compiler().compile(
            pipeline_func=pipeline_module.fraud_training_pipeline,
            package_path=str(PACKAGE),
        )
        print(f"compiled {PACKAGE}")

    
    from kfp.client import Client
    # Connect to Kubeflow
    client = Client(host=args.host)

    # Find or create the experiment "insurance-fraud"
    try:
        experiment = client.get_experiment(experiment_name=args.experiment)
    except Exception:
        experiment = client.create_experiment(
            name=args.experiment,
            description="Kubeflow runs of the fraud-detector training pipeline (CLAUDE.md §5).",
        )
    # Submit the pipeline run
    run = client.run_pipeline(
        experiment_id=experiment.experiment_id,
        job_name="fraud-model-training",
        pipeline_package_path=str(PACKAGE),
        params={"min_lift": args.min_lift},
    )
    print(f"run_id     : {run.run_id}")
    print(f"experiment : {args.experiment}")
    print(f"UI         : {args.host.replace(':8888', ':3000')}/#/runs/details/{run.run_id}")

    if not args.wait:
        return 0

    print(f"\nwaiting up to {args.timeout}s...")
    finished = client.wait_for_run_completion(run.run_id, timeout=args.timeout)
    state = finished.state
    print(f"state      : {state}")
    # KFP reports terminal state as SUCCEEDED/FAILED/CANCELED; anything that is
    # not a success is a non-zero exit so this is usable in CI (§8).
    return 0 if str(state).upper().endswith("SUCCEEDED") else 1


if __name__ == "__main__":
    sys.exit(main())
