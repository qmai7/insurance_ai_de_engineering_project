"""
Spark job: Bronze raw files -> Silver Delta Lake tables.

This script upgrades the original silver_cleaning.py from plain Parquet output
to Delta Lake output. Delta is used at the Silver layer because Silver is the
first trusted/valorized layer after raw ingestion.

Important Spark optimization choices included here:
- Adaptive Query Execution: lets Spark adjust plans at runtime.
- Skew join handling: useful because the generator intentionally creates skew.
- Partition coalescing: reduces too many tiny shuffle partitions.
- Kryo serializer: faster serialization than Java default in many Spark jobs.
- Partitioned Delta writes: improves pruning for common date/type filters.
"""

import lakehouse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def read_bronze_table(spark: SparkSession, table_name: str):
    """Read one generated Bronze Parquet file.

    policyholders is written as a folder with old/new schemas,
    so Silver uses mergeSchema to handle schema evolution.
    Other tables remain single Parquet files.
    """

    if table_name == "policyholders":
        return (
            spark.read
            # We use "mergeSchema" to reconcile the old which doesn't have "risk_segment" column with the new which does. This fills risk_segment with nulls for legacy records
            .option("mergeSchema", "true")
            .parquet(lakehouse.bronze_path("policyholders"))
        )

    return spark.read.parquet(lakehouse.bronze_path(f"{table_name}.parquet"))


def write_delta_table(df, table_name: str, partition_cols=None) -> None:
    """Write a Silver dataframe as a Delta table, optionally partitioned."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(lakehouse.silver_staging_path(table_name))


def deduplicate_by_key(df, key_column: str, order_column: str):
    """Keep the newest row per business key using a deterministic window rule."""
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def add_ingest_metadata(df, source_name: str):
    df_with_metadata = (
        df
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_system", F.lit(source_name))
        .withColumn("batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
    )

    return (
        df_with_metadata
        .withColumn("ingest_year", F.year("ingest_ts"))
        .withColumn("ingest_month", F.month("ingest_ts"))
        .withColumn("ingest_day", F.dayofmonth("ingest_ts"))
    )


def clean_policyholders(spark):
    """Standardize policyholder/customer records and deduplicate by customer_id."""
    df = read_bronze_table(spark, "policyholders")
    cleaned = (
        df.filter(F.col("customer_id").isNotNull())
        .select(
            F.col("customer_id").cast("string"),
            F.col("signup_ts").cast("timestamp"),
            F.col("age").cast("int"),
            F.col("province").cast("string"),
            F.col("city").cast("string"),
            F.col("risk_segment").cast("string"),
            F.col("marketing_opt_in").cast("boolean"),
        )
    )
    return add_ingest_metadata(deduplicate_by_key(cleaned, "customer_id", "signup_ts"), "policyholders_parquet")


def clean_policies(spark):
    """Standardize policy records and deduplicate by policy_id."""
    df = read_bronze_table(spark, "policies")
    cleaned = (
        df.filter("policy_id is not null and customer_id is not null")
        .select(
            F.col("policy_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("policy_type").cast("string"),
            F.col("policy_start_date").cast("date"),
            F.col("policy_end_date").cast("date"),
            F.col("premium_amount").cast("decimal(12,2)"),
            F.col("policy_status").cast("string"),
        )
    )
    return add_ingest_metadata(deduplicate_by_key(cleaned, "policy_id", "policy_start_date"), "policies_parquet")


def clean_claims(spark):
    """Standardize claim records, remove invalid negative measures, and deduplicate."""
    df = read_bronze_table(spark, "claims")
    cleaned = (
        df.filter("claim_id is not null and policy_id is not null")
        .select(
            F.col("claim_id").cast("string"),
            F.col("policy_id").cast("string"),
            F.col("claim_date").cast("date"),
            F.col("claim_type").cast("string"),
            F.col("claim_amount").cast("decimal(12,2)"),
            F.col("claim_status").cast("string"),
        )
        .filter(F.col("claim_amount") >= 0)
    )
    return add_ingest_metadata(deduplicate_by_key(cleaned, "claim_id", "claim_date"), "claims_parquet")


def clean_payments(spark):
    """Standardize payment attempts and create payment_dt for partition pruning."""
    df = read_bronze_table(spark, "payments")
    cleaned = (
        df.filter("payment_id is not null and policy_id is not null")
        .select(
            F.col("payment_id").cast("string"),
            F.col("policy_id").cast("string"),
            F.col("payment_date").cast("timestamp"),
            F.to_date("payment_date").alias("payment_dt"),
            F.col("amount").cast("decimal(12,2)"),
            F.col("payment_method").cast("string"),
            F.col("payment_status").cast("string"),
        )
        .filter(F.col("amount") >= 0)
    )
    return add_ingest_metadata(deduplicate_by_key(cleaned, "payment_id", "payment_date"), "payments_parquet")


def main():
    spark = lakehouse.create_spark_session("insurance_silver_delta_cleaning")
    print(f"storage: {lakehouse.describe_locations()}")

    # No directory pre-creation: object stores have no directories, and Delta
    # creates the prefix on first write. Locally, Spark does the same.

    # Each tuple contains the cleaned dataframe and the partition columns for the Delta write.
    tables = {
        "policyholders": (clean_policyholders(spark), None),
        "policies": (clean_policies(spark), ["policy_type"]),
        "claims": (clean_claims(spark), ["claim_date"]),
        "payments": (clean_payments(spark), ["payment_dt"]),
    }

    for name, (df, partitions) in tables.items():
        # Cache because we count and then write the same dataframe.
        df.cache()
        print(f"{name}: rows={df.count()}")
        write_delta_table(df, name, partitions)
        df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
