"""
Quality gate — the pipeline's decision step.

Reads the summary `ml.train` wrote and fails the pipeline if the model is not
good enough to be a promotion candidate. This is the step that makes the DAG a
pipeline rather than a script with extra steps: training always registers a
version, and this decides whether that version is worth a human looking at.

    python -m ml.gate --summary-uri gs://.../summary.json --min-pr-auc 0.20



Deliberately not a promotion step. leaves promotion manual, so passing
this gate makes a version eligible; `ml/promote.py` is still a separate, human
act. When auto-promotion is built, this is where it hooks in.
"""

from __future__ import annotations

import argparse
import json
import sys

"""
the training step writes output such as: The training step writes output such as:
- registered_model
- version
- data_version
- pr_auc
- roc_auc
- baseline_pr_auc
"""
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail the pipeline on an inadequate model.")
    parser.add_argument("--summary-uri", required=True, help="JSON written by ml.train")
    parser.add_argument(
        "--min-lift",
        type=float,
        default=2.0,
        help="minimum PR-AUC as a multiple of the base rate (default: 2.0)",
    )
    parser.add_argument(
        "--min-pr-auc",
        type=float,
        default=0.0,
        help="absolute PR-AUC floor, applied in addition to --min-lift.",
    )
    args = parser.parse_args(argv)

    summary = _read(args.summary_uri)
    # How well the model ranks claims from most likely fraud to least likely fraud
    pr_auc = float(summary["pr_auc"])
    # fraud base rate
    baseline = float(summary["baseline_pr_auc"])
    # how much better the model is than random ordering
    lift = pr_auc / baseline if baseline else 0.0

    print(f"model        : {summary.get('registered_model')} v{summary.get('version')}")
    print(f"data_version : {summary.get('data_version')}")
    print(f"pr_auc       : {pr_auc:.4f}")
    print(f"base rate    : {baseline:.4f}")
    print(f"lift         : {lift:.2f}x  (required >= {args.min_lift:.2f}x)")

    failures = []
    if lift < args.min_lift:
        failures.append(f"lift {lift:.2f}x below required {args.min_lift:.2f}x")
    if pr_auc < args.min_pr_auc:
        failures.append(f"pr_auc {pr_auc:.4f} below floor {args.min_pr_auc:.4f}")

    if failures:
        print("\nGATE FAILED")
        for failure in failures:
            print(f"  - {failure}")
        print("\nThe version stays registered — a rejected model is evidence, not garbage —"
              "\nbut it is not a promotion candidate.")
        return 1

    print("\nGATE PASSED — eligible for promotion")
    print(f"  python -m ml.promote --version {summary.get('version')}")
    return 0


def _read(uri: str) -> dict:
    if uri.startswith("gs://"):
        import gcsfs

        with gcsfs.GCSFileSystem().open(uri, "r") as handle:
            return json.load(handle)
    with open(uri) as handle:
        return json.load(handle)


if __name__ == "__main__":
    sys.exit(main())
