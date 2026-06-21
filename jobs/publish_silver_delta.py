"""
Promote validated Silver Delta staging tables to the trusted Silver Delta layer.

This script runs only after silver_quality_checks.py passes. It overwrites the
trusted Silver Delta tables with the validated candidate tables from
silver_delta_staging, partitioned by ingest_year/ingest_month/ingest_day.

Overwrite (not append) is used on purpose: the upstream source is regenerated
deterministically, so appending every run would stack identical copies of the
same data and produce many tiny Parquet files. Overwrite keeps exactly one clean
copy, and coalesce(1) keeps each partition to a single file at coursework scale.

After a successful publish, the staging folder is deleted so failed or old
candidate data cannot be accidentally reused in a future run.
"""

from pathlib import Path
import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_STAGING_DIR = BASE_DIR / "silver_delta_staging"
SILVER_TRUSTED_DIR = BASE_DIR / "silver_delta"
TABLES = ["policyholders", "policies", "claims", "payments"]
PARTITION_COLS = ["ingest_year", "ingest_month", "ingest_day"]


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("insurance_publish_silver_delta")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        # Use RawLocalFileSystem so Spark does not write hidden .crc checksum
        # sidecar files next to every file it touches. The AbstractFileSystem
        # variant covers Delta's transaction-log writes, which go through the
        # Hadoop FileContext API and would otherwise still emit a .crc per commit.
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def publish_table(spark: SparkSession, table_name: str) -> None:
    staging_path = SILVER_STAGING_DIR / table_name
    trusted_path = SILVER_TRUSTED_DIR / table_name

    if not staging_path.exists():
        raise FileNotFoundError(f"Cannot publish missing staging table: {staging_path}")

    df = spark.read.format("delta").load(str(staging_path))
    row_count = df.count()
    print(f"Publishing {table_name}: rows={row_count}")

    (
        df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*PARTITION_COLS)
        .save(str(trusted_path))
    )


def main() -> None:
    if not SILVER_STAGING_DIR.exists():
        raise FileNotFoundError(f"Missing staging folder: {SILVER_STAGING_DIR}")

    SILVER_TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session()

    for table_name in TABLES:
        publish_table(spark, table_name)

    spark.stop()

    # Clear staging only after every table has been published successfully, so
    # stale candidate data cannot be reused by a later run. We empty the
    # directory's contents instead of deleting the directory itself, because
    # silver_delta_staging is a bind-mounted volume (see docker-compose.yml) and
    # removing the mount point raises OSError: [Errno 16] Device or resource busy.
    for child in SILVER_STAGING_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    print(f"Published trusted Silver Delta tables to: {SILVER_TRUSTED_DIR}")
    print(f"Cleared staging contents in: {SILVER_STAGING_DIR}")


if __name__ == "__main__":
    main()