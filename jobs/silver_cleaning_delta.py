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

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "generated_insurance_data" / "offline"
SILVER_STAGING_DIR = BASE_DIR / "silver_delta_staging"


def create_spark_session() -> SparkSession:
    """Create a local Spark session with Delta Lake and tuning options enabled."""
    builder = (
        SparkSession.builder
        .appName("insurance_silver_delta_cleaning")
        .master("local[*]")
        # Required Delta Lake configs.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Spark optimization/tuning configs from the course.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.autoBroadcastJoinThreshold", "20MB")
        # Use RawLocalFileSystem so Spark does not write a hidden .crc checksum
        # sidecar next to every file. Harmless on local coursework storage and
        # keeps the Delta folders far less cluttered. The AbstractFileSystem
        # variant covers Delta's transaction-log writes (Hadoop FileContext API).
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_bronze_table(spark: SparkSession, table_name: str):
    """Read one generated Bronze Parquet file."""
    return spark.read.parquet(str(BRONZE_DIR / f"{table_name}.parquet"))


def write_delta_table(df, table_name: str, partition_cols=None) -> None:
    """Write a Silver dataframe as a Delta table, optionally partitioned."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(str(SILVER_STAGING_DIR / table_name))


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
    spark = create_spark_session()
    SILVER_STAGING_DIR.mkdir(parents=True, exist_ok=True)

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
