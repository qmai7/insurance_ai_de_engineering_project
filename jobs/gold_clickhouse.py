"""
Spark job: Silver Delta Lake -> Gold ClickHouse warehouse.

ClickHouse is used for the Gold layer because Gold is mostly analytical:
BI queries, aggregations, fact tables, OBT tables, and offline feature tables.
This is a better OLAP fit than PostgreSQL for read-heavy warehouse workloads.

Pipeline position:
Bronze raw files -> Silver Delta Lake -> Silver quality gate -> Gold ClickHouse.

Gold objects created:
- dim_customer
- dim_policy
- dim_date
- fact_claims
- fact_payment_attempts
- obt_claims_enriched
- feat_customer_90d
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple

import clickhouse_connect
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "silver_delta"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")


def spark_session() -> SparkSession:
    """Create Spark with Delta support and local-coursework optimization settings."""
    builder = (
        SparkSession.builder.appName("gold_clickhouse_modeling")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        # Use RawLocalFileSystem so Spark does not write hidden .crc checksum
        # sidecar files when reading/writing local Delta tables. The
        # AbstractFileSystem variant covers Delta's transaction-log writes.
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta(spark: SparkSession, name: str):
    """Read one trusted Silver Delta table by folder name."""
    return spark.read.format("delta").load(str(SILVER_DIR / name))


def clickhouse_client():
    """Create a ClickHouse HTTP client used to create tables and load data."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def recreate_and_insert(client, table_name: str, create_sql: str, spark_df) -> None:
    """
    Recreate a ClickHouse table and insert a Spark dataframe into it.

    For coursework scale, converting Spark -> pandas is acceptable and keeps the
    project easy to run. In production, large loads should use a distributed
    connector or write files to object storage and let ClickHouse ingest them.
    """
    full_table = f"{CLICKHOUSE_DATABASE}.{table_name}"
    client.command(f"DROP TABLE IF EXISTS {full_table}")
    client.command(create_sql)

    pdf = spark_df.toPandas()
    if len(pdf) > 0:
        client.insert_df(full_table, pdf)
    print(f"loaded {full_table}: {len(pdf)} rows")


def main() -> None:
    spark = spark_session()
    client = clickhouse_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")

    # Load trusted Silver Delta tables. Airflow runs the Silver quality gate first.
    ph = read_delta(spark, "policyholders")
    pol = read_delta(spark, "policies")
    claims = read_delta(spark, "claims")
    payments = read_delta(spark, "payments")

    # Dimension: one row per customer. customer_key is a warehouse surrogate key.
    dim_customer = ph.selectExpr(
        "cast(dense_rank() over(order by customer_id) as long) as customer_key",
        "customer_id",
        "signup_ts",
        "cast(age as int) as age",
        "province",
        "city",
        "risk_segment",
        "marketing_opt_in",
    )

    # Dimension: one row per policy. It links to dim_customer through customer_key.
    dim_policy = (
        pol.join(dim_customer.select("customer_id", "customer_key"), "customer_id", "left")
        .selectExpr(
            "cast(dense_rank() over(order by policy_id) as long) as policy_key",
            "policy_id",
            "cast(customer_key as long) as customer_key",
            "policy_type",
            "policy_start_date",
            "policy_end_date",
            "cast(premium_amount as double) as premium_amount",
            "policy_status",
        )
    )

    # Date dimension: collect all business dates used by policies, claims, and payments.
    date_df = (
        pol.select(F.col("policy_start_date").alias("calendar_date"))
        .union(pol.select(F.col("policy_end_date").alias("calendar_date")))
        .union(claims.select(F.col("claim_date").alias("calendar_date")))
        .union(payments.select(F.to_date("payment_date").alias("calendar_date")))
        .where("calendar_date is not null")
        .distinct()
    )
    dim_date = date_df.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("calendar_date"),
        F.year("calendar_date").cast("int").alias("year"),
        F.month("calendar_date").cast("int").alias("month"),
        F.dayofmonth("calendar_date").cast("int").alias("day"),
        F.dayofweek("calendar_date").cast("int").alias("day_of_week"),
        (F.dayofweek("calendar_date").isin(1, 7)).alias("is_weekend"),
    )

    # Fact: one row per claim. Small dimensions are broadcast for efficient joins.
    fact_claims = (
        claims.join(F.broadcast(dim_policy.select("policy_id", "policy_key", "customer_key")), "policy_id", "left")
        .join(
            F.broadcast(dim_date.select(F.col("calendar_date").alias("claim_date"), F.col("date_key").alias("claim_date_key"))),
            "claim_date",
            "left",
        )
        .selectExpr(
            "claim_id",
            "cast(customer_key as long) as customer_key",
            "cast(policy_key as long) as policy_key",
            "cast(claim_date_key as int) as claim_date_key",
            "claim_type",
            "claim_status",
            "cast(claim_amount as double) as claim_amount",
        )
    )

    # Fact: one row per payment attempt, including failed attempts.
    fact_payment_attempts = (
        payments.join(F.broadcast(dim_policy.select("policy_id", "policy_key", "customer_key")), "policy_id", "left")
        .withColumn("payment_calendar_date", F.to_date("payment_date"))
        .join(
            F.broadcast(dim_date.select(F.col("calendar_date").alias("payment_calendar_date"), F.col("date_key").alias("payment_date_key"))),
            "payment_calendar_date",
            "left",
        )
        .selectExpr(
            "payment_id",
            "cast(customer_key as long) as customer_key",
            "cast(policy_key as long) as policy_key",
            "cast(payment_date_key as int) as payment_date_key",
            "payment_method",
            "payment_status",
            "cast(amount as double) as amount",
        )
    )

    # OBT: transaction-grain (one row per claim) denormalized table for claim/loss
    # BI and dashboards. Joins claim -> policy -> customer -> date so analytical
    # queries (by type, status, geography, time, loss ratio) need no joins.
    obt_claims_enriched = (
        claims.select("claim_id", "policy_id", "claim_date", "claim_type", "claim_status", "claim_amount")
        .join(
            pol.select(
                "policy_id",
                "customer_id",
                "policy_type",
                "policy_status",
                F.col("premium_amount").cast("double").alias("premium_amount"),
                "policy_start_date",
                "policy_end_date",
            ),
            "policy_id",
            "left",
        )
        .join(
            ph.select("customer_id", "province", "city", "risk_segment", "age", "marketing_opt_in"),
            "customer_id",
            "left",
        )
        .join(
            F.broadcast(
                dim_date.select(
                    F.col("calendar_date").alias("claim_date"),
                    F.col("year").alias("claim_year"),
                    F.col("month").alias("claim_month"),
                    F.col("day_of_week").alias("claim_day_of_week"),
                    F.col("is_weekend").alias("claim_is_weekend"),
                )
            ),
            "claim_date",
            "left",
        )
        .selectExpr(
            "claim_id",
            "claim_date",
            "claim_type",
            "claim_status",
            "cast(claim_amount as double) as claim_amount",
            "policy_id",
            "policy_type",
            "policy_status",
            "premium_amount",
            "policy_start_date",
            "policy_end_date",
            "case when premium_amount > 0 then round(cast(claim_amount as double) / premium_amount, 4) end as claim_to_premium_ratio",
            "customer_id",
            "province",
            "city",
            "risk_segment",
            "cast(age as int) as age",
            "marketing_opt_in",
            "cast(claim_year as int) as claim_year",
            "cast(claim_month as int) as claim_month",
            "cast(claim_day_of_week as int) as claim_day_of_week",
            "claim_is_weekend",
        )
    )

    # Offline feature table. Later, this can become a Feast offline store source.
    as_of = F.to_date(F.lit("2025-11-01"))
    claims_90 = (
        fact_claims.join(dim_policy.select("policy_key", "policy_type"), "policy_key")
        .join(dim_date.select(F.col("date_key").alias("claim_date_key"), "calendar_date"), "claim_date_key")
        .where((F.col("calendar_date") >= F.date_sub(as_of, 90)) & (F.col("calendar_date") < as_of))
    )
    payments_90 = (
        fact_payment_attempts.join(dim_date.select(F.col("date_key").alias("payment_date_key"), "calendar_date"), "payment_date_key")
        .where((F.col("calendar_date") >= F.date_sub(as_of, 90)) & (F.col("calendar_date") < as_of))
    )

    claim_features = claims_90.groupBy("customer_key").agg(
        F.coalesce(F.avg("claim_amount"), F.lit(0.0)).alias("f_customer_avg_claim_amount_90d"),
        F.count("claim_id").cast("int").alias("f_customer_total_claims_90d"),
        F.coalesce(F.sum("claim_amount"), F.lit(0.0)).alias("f_customer_total_claim_amount_90d"),
    )
    payment_features = payments_90.groupBy("customer_key").agg(
        F.coalesce(F.sum("amount"), F.lit(0.0)).alias("f_customer_total_payments_90d"),
        F.coalesce(F.avg(F.when(F.col("payment_status") == "failed", 1.0).otherwise(0.0)), F.lit(0.0)).alias(
            "f_customer_payment_failure_rate_90d"
        ),
    )
    feat_customer_90d = (
        dim_customer.select("customer_key", "customer_id")
        .join(claim_features, "customer_key", "left")
        .join(payment_features, "customer_key", "left")
        .fillna(0)
        .withColumn("as_of_date", as_of)
        .drop("customer_key")
    )

    table_ddls: Dict[str, Tuple[str, object]] = {
        "dim_customer": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.dim_customer
            (
                customer_key UInt64,
                customer_id String,
                signup_ts DateTime,
                age UInt16,
                province LowCardinality(String),
                city LowCardinality(String),
                risk_segment Nullable(String),
                marketing_opt_in Bool
            ) ENGINE = MergeTree
            ORDER BY customer_id
            """,
            dim_customer,
        ),
        "dim_policy": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.dim_policy
            (
                policy_key UInt64,
                policy_id String,
                customer_key UInt64,
                policy_type LowCardinality(String),
                policy_start_date Date,
                policy_end_date Date,
                premium_amount Float64,
                policy_status LowCardinality(String)
            ) ENGINE = MergeTree
            ORDER BY (policy_type, policy_id)
            """,
            dim_policy,
        ),
        "dim_date": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.dim_date
            (
                date_key UInt32,
                calendar_date Date,
                year UInt16,
                month UInt8,
                day UInt8,
                day_of_week UInt8,
                is_weekend Bool
            ) ENGINE = MergeTree
            ORDER BY date_key
            """,
            dim_date,
        ),
        "fact_claims": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.fact_claims
            (
                claim_id String,
                customer_key Nullable(UInt64),
                policy_key Nullable(UInt64),
                claim_date_key Nullable(UInt32),
                claim_type LowCardinality(String),
                claim_status LowCardinality(String),
                claim_amount Float64
            ) ENGINE = MergeTree
            PARTITION BY intDiv(ifNull(claim_date_key, 0), 100)
            ORDER BY (ifNull(claim_date_key, 0), ifNull(customer_key, 0), ifNull(policy_key, 0), claim_id)
            """,
            fact_claims,
        ),
        "fact_payment_attempts": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.fact_payment_attempts
            (
                payment_id String,
                customer_key Nullable(UInt64),
                policy_key Nullable(UInt64),
                payment_date_key Nullable(UInt32),
                payment_method Nullable(String),
                payment_status LowCardinality(String),
                amount Float64
            ) ENGINE = MergeTree
            PARTITION BY intDiv(ifNull(payment_date_key, 0), 100)
            ORDER BY (ifNull(payment_date_key, 0), ifNull(customer_key, 0), ifNull(policy_key, 0), payment_id)
            """,
            fact_payment_attempts,
        ),
        "obt_claims_enriched": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.obt_claims_enriched
            (
                claim_id String,
                claim_date Date,
                claim_type LowCardinality(String),
                claim_status LowCardinality(String),
                claim_amount Float64,
                policy_id String,
                policy_type LowCardinality(String),
                policy_status LowCardinality(String),
                premium_amount Float64,
                policy_start_date Date,
                policy_end_date Date,
                claim_to_premium_ratio Nullable(Float64),
                customer_id String,
                province LowCardinality(String),
                city LowCardinality(String),
                risk_segment Nullable(String),
                age UInt16,
                marketing_opt_in Bool,
                claim_year UInt16,
                claim_month UInt8,
                claim_day_of_week UInt8,
                claim_is_weekend Bool
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(claim_date)
            ORDER BY (claim_date, policy_type, province, claim_id)
            """,
            obt_claims_enriched,
        ),
        "feat_customer_90d": (
            f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.feat_customer_90d
            (
                customer_id String,
                f_customer_avg_claim_amount_90d Float64,
                f_customer_total_claims_90d UInt32,
                f_customer_total_claim_amount_90d Float64,
                f_customer_total_payments_90d Float64,
                f_customer_payment_failure_rate_90d Float64,
                as_of_date Date
            ) ENGINE = MergeTree
            ORDER BY (as_of_date, customer_id)
            """,
            feat_customer_90d,
        ),
    }

    for table_name, (ddl, df) in table_ddls.items():
        recreate_and_insert(client, table_name, ddl, df)

    spark.stop()


if __name__ == "__main__":
    main()
