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

import lakehouse
from pyspark.sql import SparkSession

TABLES = lakehouse.SILVER_TABLES
PARTITION_COLS = ["ingest_year", "ingest_month", "ingest_day"]


def publish_table(spark: SparkSession, table_name: str) -> None:
    staging_path = lakehouse.silver_staging_path(table_name)
    trusted_path = lakehouse.silver_trusted_path(table_name)

    if not lakehouse.path_exists(spark, staging_path):
        raise FileNotFoundError(f"Cannot publish missing staging table: {staging_path}")

    df = spark.read.format("delta").load(staging_path)
    row_count = df.count()
    print(f"Publishing {table_name}: rows={row_count}")

    (
        df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*PARTITION_COLS)
        .save(trusted_path)
    )


def main() -> None:
    spark = lakehouse.create_spark_session("insurance_publish_silver_delta")
    print(f"storage: {lakehouse.describe_locations()}")

    staging_root = lakehouse.silver_staging_path()
    if not lakehouse.path_exists(spark, staging_root):
        raise FileNotFoundError(f"Missing staging location: {staging_root}")

    for table_name in TABLES:
        publish_table(spark, table_name)
    
    # Clear staging only after every table has been published successfully, so
    # stale candidate data cannot be reused by a later run. Children are removed
    # individually rather than deleting the root: locally the root is a
    # bind-mounted volume and unlinking the mount point raises EBUSY, and on GCS
    # keeping the prefix stable avoids surprising a later reader with a
    # nonexistent path.
    #
    # Done before spark.stop() — these deletes go through the Hadoop FileSystem
    # API, which needs the session's Hadoop configuration for GCS credentials.
    for child in lakehouse.list_children(spark, staging_root):
        lakehouse.delete_path(spark, child)

    spark.stop()

    print(f"Published trusted Silver Delta tables to: {lakehouse.silver_trusted_path()}")
    print(f"Cleared staging contents in: {staging_root}")


if __name__ == "__main__":
    main()