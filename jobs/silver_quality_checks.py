"""
Silver quality gate for the Airflow pipeline.

Purpose:
- Validate cleaned Silver Delta Lake tables before Gold modeling starts.
- Fail fast when required keys are null, duplicate IDs remain, or measures are invalid.

This script is intentionally simple and readable for coursework. In a larger
production project, the same checks could be implemented with Great Expectations
or Deequ, and results could be published to a data quality dashboard.
"""

import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_STAGING_DIR = BASE_DIR / "silver_delta_staging"


def create_spark_session() -> SparkSession:
    """Create Spark with Delta support so we can read Silver Delta tables."""
    builder = (
        SparkSession.builder
        .appName("silver_delta_quality_gate")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Use RawLocalFileSystem so Spark does not write hidden .crc checksum
        # sidecar files next to every file it touches. The AbstractFileSystem
        # variant covers Delta's transaction-log writes (Hadoop FileContext API).
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta(spark: SparkSession, table_name: str):
    """Read one Silver table from the local Delta Lake folder."""
    return spark.read.format("delta").load(str(SILVER_STAGING_DIR / table_name))


def duplicate_count(df, key_column: str) -> int:
    """Return number of duplicated business keys remaining after Silver cleaning."""
    return df.groupBy(key_column).count().where(F.col("count") > 1).count()


def main() -> None:
    spark = create_spark_session()

    tables = {
        "policyholders": (read_delta(spark, "policyholders"), "customer_id"),
        "policies": (read_delta(spark, "policies"), "policy_id"),
        "claims": (read_delta(spark, "claims"), "claim_id"),
        "payments": (read_delta(spark, "payments"), "payment_id"),
    }

    checks = []

    for table_name, (df, key_col) in tables.items():
        # Every Silver table must have rows, a non-null primary/business key,
        # and no duplicate business keys after deduplication.
        checks.append((f"{table_name}_row_count_positive", df.count() > 0))
        checks.append((f"{table_name}_{key_col}_not_null", df.where(F.col(key_col).isNull()).count() == 0))
        checks.append((f"{table_name}_{key_col}_unique", duplicate_count(df, key_col) == 0))

    # Domain-specific measure checks.
    checks.append(("claims_amount_non_negative", tables["claims"][0].where(F.col("claim_amount") < 0).count() == 0))
    checks.append(("payments_amount_non_negative", tables["payments"][0].where(F.col("amount") < 0).count() == 0))

    failed = [name for name, passed in checks if not passed]
    for name, passed in checks:
        print(f"{name}: {'PASS' if passed else 'FAIL'}")

    spark.stop()

    # Airflow treats a non-zero exit code as a failed task, so this becomes the quality gate.
    if failed:
        print("Silver quality gate failed:", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
