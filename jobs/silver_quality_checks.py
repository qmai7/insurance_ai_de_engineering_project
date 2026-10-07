"""
Silver quality gate for the Airflow pipeline.

Purpose:
- Validate cleaned Silver Delta Lake tables before Gold modeling starts.
- Fail fast when required keys are null, duplicate IDs remain, or measures are invalid.

This script is intentionally simple and readable for coursework. In a larger
production project, the same checks could be implemented with Great Expectations
or Deequ, and results could be published to a data quality dashboard.
"""

import json
import sys

import lakehouse
from pyspark.sql import SparkSession, functions as F


def read_delta(spark: SparkSession, table_name: str):
    """Read one candidate Silver table from the staging layer."""
    return spark.read.format("delta").load(lakehouse.silver_staging_path(table_name))


def duplicate_count(df, key_column: str) -> int:
    """Return number of duplicated business keys remaining after Silver cleaning."""
    return df.groupBy(key_column).count().where(F.col("count") > 1).count()


def main() -> None:
    spark = lakehouse.create_spark_session("silver_delta_quality_gate")
    print(f"storage: {lakehouse.describe_locations()}")

    tables = {
        "policyholders": (read_delta(spark, "policyholders"), "customer_id"),
        "policies": (read_delta(spark, "policies"), "policy_id"),
        "claims": (read_delta(spark, "claims"), "claim_id"),
        "payments": (read_delta(spark, "payments"), "payment_id"),
    }

    checks = []
    row_counts = {}

    for table_name, (df, key_col) in tables.items():
        # Every Silver table must have rows, a non-null primary/business key,
        # and no duplicate business keys after deduplication.
        row_count = df.count()
        row_counts[table_name] = row_count
        checks.append((f"{table_name}_row_count_positive", row_count > 0))
        checks.append((f"{table_name}_{key_col}_not_null", df.where(F.col(key_col).isNull()).count() == 0))
        checks.append((f"{table_name}_{key_col}_unique", duplicate_count(df, key_col) == 0))

    # Domain-specific measure checks.
    checks.append(("claims_amount_non_negative", tables["claims"][0].where(F.col("claim_amount") < 0).count() == 0))
    checks.append(("payments_amount_non_negative", tables["payments"][0].where(F.col("amount") < 0).count() == 0))

    failed = [name for name, passed in checks if not passed]
    for name, passed in checks:
        print(f"{name}: {'PASS' if passed else 'FAIL'}")

    # Persist real results so DataHub can publish the actual outcome of this
    # gate instead of a placeholder (read by publish_datahub_lineage.py).
    #
    # This goes to the lakehouse, not local disk. The task that reads it back is
    # a different Airflow task, which on Kubernetes means a different pod with a
    # different filesystem — a local file here would simply not exist by then.
    report_uri = lakehouse.report_path("silver_quality_report.json")
    lakehouse.write_text(spark, report_uri, json.dumps({
        "checks": {name: passed for name, passed in checks},
        "row_counts": row_counts,
    }, indent=2))
    print(f"Wrote quality report to {report_uri}")

    spark.stop()

    # Airflow treats a non-zero exit code as a failed task, so this becomes the quality gate.
    if failed:
        print("Silver quality gate failed:", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()