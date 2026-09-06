"""
Bronze input gate: fail fast when source data is missing.

Part 1 did this with `test -f` on local paths inside the Airflow container. That
cannot work now — Bronze lives in GCS and the task runs in a pod that never had
those files. Worse than failing, a hardcoded local check could in principle pass
while the Spark jobs read something else entirely, so the check now resolves the
same location the jobs themselves use.

Existence is checked through the Hadoop FileSystem API rather than a GCS client
so this works unchanged against local disk or a bucket, and reuses the exact
credential path the Spark jobs use — if auth is broken, it fails here with a
clear message instead of midway through the first Spark read.
"""

from __future__ import annotations

import sys

import lakehouse

# policyholders is a folder (part_old + part_new drive the schema-evolution case);
# the rest are single Parquet files.
REQUIRED = [
    "policyholders/part_old.parquet",
    "policyholders/part_new.parquet",
    "policies.parquet",
    "claims.parquet",
    "payments.parquet",
]


def main() -> None:
    spark = lakehouse.create_spark_session("validate_bronze_inputs")
    print(f"storage: {lakehouse.describe_locations()}")
    print("argo-cd-gitops-test: this line only exists after the new image deployed")

    missing = []
    for relative in REQUIRED:
        uri = lakehouse.bronze_path(relative)
        if lakehouse.path_exists(spark, uri):
            print(f"OK      {uri}")
        else:
            print(f"MISSING {uri}")
            missing.append(uri)

    spark.stop()

    if missing:
        print(f"\nBronze validation failed; {len(missing)} input(s) missing:")
        for uri in missing:
            print(f"  - {uri}")
        print(
            "\nGenerate the source data and upload it, e.g.:\n"
            "  python jobs/insurance_data_generator.py\n"
            f"  gcloud storage cp -r generated_insurance_data/offline/* {lakehouse.bronze_path()}/"
        )
        # Non-zero exit is what makes this an Airflow gate.
        sys.exit(1)

    print(f"\nAll {len(REQUIRED)} Bronze inputs present. Continuing pipeline.")


if __name__ == "__main__":
    main()
