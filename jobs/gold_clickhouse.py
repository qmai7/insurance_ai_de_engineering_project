"""
Spark job: Silver Delta Lake -> Gold ClickHouse warehouse.

ClickHouse is used for the Gold layer because Gold is mostly analytical:
BI queries, aggregations, fact tables, OBT tables, and offline feature tables.
This is a better OLAP fit than PostgreSQL for read-heavy warehouse workloads.

Pipeline position:
Bronze raw files -> Silver Delta Lake -> Silver quality gate -> Gold ClickHouse.

Gold objects created:
- dim_customer   (Slowly Changing Dimension Type 2: full version history)
- dim_policy
- dim_date
- fact_claims
- fact_payment_attempts
- obt_claims_enriched
- feat_customer_90d

dim_customer is modelled as an SCD Type 2 dimension. Instead of overwriting a
customer row when an attribute changes, each run compares the incoming Silver
snapshot against the version currently stored in ClickHouse. Changed customers
get their old row closed (is_current = false, valid_to_ts set) and a brand-new
versioned row opened (is_current = true). This preserves the full history of how
customer attributes (province, city, risk_segment, marketing_opt_in, age) evolve
over time, which Type-1 overwrite would destroy.

Because ClickHouse has no cheap row-level UPDATE, the merge is computed in the
job (read prior versions -> merge -> rewrite the whole dimension). The prior
history is always read back before the table is rewritten, so no version is lost.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import clickhouse_connect
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

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


def reset_managed_table(spark: SparkSession, table_name: str) -> None:
    """
    Make a managed (saveAsTable) table safe to recreate on every run.

    Spark's local warehouse keeps table *data* on disk, but the Derby metastore
    is recreated fresh each job. So on the second run the catalog is empty while
    the directory still exists, and saveAsTable fails with LOCATION_ALREADY_EXISTS.
    Dropping the (possibly unregistered) table and deleting its warehouse
    directory clears both sides, so bucketed writes are cleanly re-runnable.
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{warehouse_dir}/{table_name}")
    fs = path.getFileSystem(hadoop_conf)
    if fs.exists(path):
        fs.delete(path, True)  # recursive


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


# ---------------------------------------------------------------------------
# SCD Type 2: dim_customer
# ---------------------------------------------------------------------------

# Business (natural) key that identifies the same customer across versions.
SCD2_BUSINESS_KEY = "customer_id"

# Attributes tracked for change detection. When any of these change for a
# customer, the current version is closed and a new version is opened.
SCD2_TRACKED_ATTRS = ["age", "province", "city", "risk_segment", "marketing_opt_in"]

# Full column list stored in the dimension (surrogate key + attributes + SCD2 metadata).
SCD2_COLUMNS = [
    "customer_key",   # surrogate key, unique per *version*
    "customer_id",    # business key
    "signup_ts",
    "age",
    "province",
    "city",
    "risk_segment",
    "marketing_opt_in",
    "row_hash",       # hash of tracked attrs, used for change detection
    "valid_from_ts",  # when this version became effective
    "valid_to_ts",    # when this version was superseded (NULL while current)
    "is_current",     # convenience flag: True for the live version
]

DIM_CUSTOMER_DDL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.dim_customer
(
    customer_key UInt64,
    customer_id String,
    signup_ts DateTime,
    age UInt16,
    province LowCardinality(String),
    city LowCardinality(String),
    risk_segment Nullable(String),
    marketing_opt_in Bool,
    row_hash String,
    valid_from_ts DateTime,
    valid_to_ts Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (customer_id, valid_from_ts)
"""


def _hash_attrs(record: dict) -> str:
    """Stable hash of the tracked attributes, used to detect changes."""
    parts = ["" if record.get(a) is None else str(record.get(a)) for a in SCD2_TRACKED_ATTRS]
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


def _read_existing_dim_customer(client) -> List[dict]:
    """
    Read every stored version of dim_customer (empty list on first run).

    If a legacy Type-1 dim_customer exists (no SCD2 metadata columns), it is
    dropped and treated as a fresh initial load, so the switch to SCD2 is a
    one-time automatic migration rather than a manual step.
    """
    columns = {
        row[0]
        for row in client.query(
            "SELECT name FROM system.columns "
            f"WHERE database = '{CLICKHOUSE_DATABASE}' AND table = 'dim_customer'"
        ).result_rows
    }
    if not columns:
        return []
    if "is_current" not in columns:
        # Legacy Type-1 table -> drop it and rebuild as SCD2 from scratch.
        client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.dim_customer")
        return []
    result = client.query(
        f"SELECT {', '.join(SCD2_COLUMNS)} FROM {CLICKHOUSE_DATABASE}.dim_customer"
    )
    return [dict(zip(SCD2_COLUMNS, row)) for row in result.result_rows]


def _compute_scd2_merge(prior: List[dict], incoming: Dict[str, dict], run_ts: datetime):
    """
    Pure SCD Type 2 transition logic (no Spark / ClickHouse), so it can be tested.

    prior    -- every version currently stored (each dict has the SCD2_COLUMNS).
    incoming -- {customer_id: attribute-record-with-row_hash} for the new snapshot.
    Returns (merged_versions, stats).
    """
    prior_current = {r[SCD2_BUSINESS_KEY]: r for r in prior if r["is_current"]}
    max_key = max((int(r["customer_key"]) for r in prior), default=0)

    merged: List[dict] = []
    # Untouched history: any version already closed stays exactly as-is.
    merged.extend(r for r in prior if not r["is_current"])

    def _open_version(rec: dict, key: int) -> dict:
        return {
            "customer_key": key,
            "customer_id": rec["customer_id"],
            "signup_ts": rec["signup_ts"],
            "age": rec["age"],
            "province": rec["province"],
            "city": rec["city"],
            "risk_segment": rec["risk_segment"],
            "marketing_opt_in": bool(rec["marketing_opt_in"]),
            "row_hash": rec["row_hash"],
            "valid_from_ts": run_ts,
            "valid_to_ts": None,
            "is_current": True,
        }

    def _close_version(row: dict) -> dict:
        closed_row = dict(row)
        closed_row["valid_to_ts"] = run_ts
        closed_row["is_current"] = False
        return closed_row

    stats = {"new": 0, "unchanged": 0, "changed": 0, "expired": 0}
    # Process incoming customers in business-key order so new surrogate keys are
    # assigned deterministically regardless of dict iteration order.
    for cid in sorted(incoming):
        rec = incoming[cid]
        current = prior_current.get(cid)
        if current is None:
            # Brand-new customer -> open first version.
            max_key += 1
            merged.append(_open_version(rec, max_key))
            stats["new"] += 1
        elif current["row_hash"] == rec["row_hash"]:
            # No tracked attribute changed -> keep the current version live.
            merged.append(current)
            stats["unchanged"] += 1
        else:
            # Tracked attribute changed -> close old version, open a new one.
            merged.append(_close_version(current))
            max_key += 1
            merged.append(_open_version(rec, max_key))
            stats["changed"] += 1

    # Customers that disappeared from the source are soft-closed (SCD2 expiry).
    for cid, current in prior_current.items():
        if cid not in incoming:
            merged.append(_close_version(current))
            stats["expired"] += 1

    return merged, stats


def scd2_merge_dim_customer(client, spark, ph, run_ts: datetime):
    """
    Merge the incoming Silver customer snapshot into the SCD Type 2 dim_customer.

    Returns a small Spark dataframe (customer_key, customer_id) for the *current*
    version of every customer, so downstream dim_policy / feature joins keep
    resolving one surrogate key per customer.
    """
    # Incoming snapshot: Silver already deduplicates to one row per customer_id.
    incoming_pdf = ph.selectExpr(
        "cast(customer_id as string) as customer_id",
        "signup_ts",
        "cast(age as int) as age",
        "province",
        "city",
        "risk_segment",
        "marketing_opt_in",
    ).toPandas()
    incoming = {}
    for rec in incoming_pdf.to_dict("records"):
        rec["row_hash"] = _hash_attrs(rec)
        incoming[rec[SCD2_BUSINESS_KEY]] = rec

    prior = _read_existing_dim_customer(client)
    merged, stats = _compute_scd2_merge(prior, incoming, run_ts)

    # Rewrite the full dimension. History was read above, so nothing is lost.
    client.command(DIM_CUSTOMER_DDL)
    client.command(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.dim_customer")
    rows = [[r[c] for c in SCD2_COLUMNS] for r in merged]
    if rows:
        client.insert(
            f"{CLICKHOUSE_DATABASE}.dim_customer",
            rows,
            column_names=SCD2_COLUMNS,
        )
    print(
        f"dim_customer SCD2: {len(merged)} total versions "
        f"(new={stats['new']}, unchanged={stats['unchanged']}, "
        f"changed={stats['changed']}, expired={stats['expired']})"
    )

    # Current-version key map for downstream joins.
    current_rows = [
        (int(r["customer_key"]), str(r["customer_id"]))
        for r in merged
        if r["is_current"]
    ]
    schema = StructType([
        StructField("customer_key", LongType(), False),
        StructField("customer_id", StringType(), False),
    ])
    return spark.createDataFrame(current_rows, schema)


def main() -> None:
    spark = spark_session()
    client = clickhouse_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")

    # Load trusted Silver Delta tables. Airflow runs the Silver quality gate first.
    ph = read_delta(spark, "policyholders")
    pol = read_delta(spark, "policies")
    claims = read_delta(spark, "claims")
    payments = read_delta(spark, "payments")

    # Single processing timestamp so every version opened/closed this run agrees.
    run_ts = datetime.utcnow().replace(microsecond=0)

    # Dimension: SCD Type 2. This reads the prior versions from ClickHouse, opens
    # new versions for changed/new customers, closes superseded ones, and rewrites
    # the whole dimension. It returns the current-version surrogate key per
    # customer, which downstream policy/feature joins use so each customer still
    # resolves to exactly one customer_key.
    dim_customer_curr = scd2_merge_dim_customer(client, spark, ph, run_ts)

    # Dimension: one row per policy. It links to dim_customer through the current
    # customer_key.
    dim_policy = (
        pol.join(dim_customer_curr.select("customer_id", "customer_key"), "customer_id", "left")
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

    # High-cardinality optimization:
    # Bucket fact tables by customer_key before customer-level feature aggregation.
    # This pre-organizes rows with the same customer_key into the same bucket hash partition, 
    # which allows Spark to skip the shuffle stage when grouping by customer_key.

    BUCKETS = 8

    # Clear any orphaned warehouse location so re-runs (e.g. Airflow re-triggers)
    # don't fail with LOCATION_ALREADY_EXISTS on these managed bucketed tables.
    reset_managed_table(spark, "fact_claims_bucketed")
    reset_managed_table(spark, "fact_payment_attempts_bucketed")

    fact_claims.write \
        .mode("overwrite") \
        .bucketBy(BUCKETS, "customer_key") \
        .sortBy("customer_key") \
        .saveAsTable("fact_claims_bucketed")

    fact_payment_attempts.write \
        .mode("overwrite") \
        .bucketBy(BUCKETS, "customer_key") \
        .sortBy("customer_key") \
        .saveAsTable("fact_payment_attempts_bucketed")

    fact_claims_bucketed = spark.table("fact_claims_bucketed")
    fact_payment_attempts_bucketed = spark.table("fact_payment_attempts_bucketed")

    claims_90 = (
        fact_claims_bucketed
        .join(dim_policy.select("policy_key", "policy_type"), "policy_key")
        .join(dim_date.select(F.col("date_key").alias("claim_date_key"), "calendar_date"), "claim_date_key")
        .where((F.col("calendar_date") >= F.date_sub(as_of, 90)) & (F.col("calendar_date") < as_of))
    )
    
    payments_90 = (
        fact_payment_attempts_bucketed
        .join(dim_date.select(F.col("date_key").alias("payment_date_key"), "calendar_date"), "payment_date_key")
        .where((F.col("calendar_date") >= F.date_sub(as_of, 90)) & (F.col("calendar_date") < as_of))
    )

    # Aggregate claim features by customer_key for the last 90 days. 
    claim_features = claims_90.groupBy("customer_key").agg(
        #Use coalesce to fill nulls with 0
        F.coalesce(F.avg("claim_amount"), F.lit(0.0)).alias("f_customer_avg_claim_amount_90d"),
        F.count("claim_id").cast("int").alias("f_customer_total_claims_90d"),
        F.coalesce(F.sum("claim_amount"), F.lit(0.0)).alias("f_customer_total_claim_amount_90d"),
    )

    # Aggregate payment features by customer_key for the last 90 days.
    payment_features = payments_90.groupBy("customer_key").agg(
        F.coalesce(F.sum("amount"), F.lit(0.0)).alias("f_customer_total_payments_90d"),
        F.coalesce(
            F.avg(F.when(F.col("payment_status") == "failed", 1.0).otherwise(0.0)),
            F.lit(0.0)
        ).alias("f_customer_payment_failure_rate_90d"),
    )

    feat_customer_90d = (
        dim_customer_curr.select("customer_key", "customer_id")
        .join(claim_features, "customer_key", "left")
        .join(payment_features, "customer_key", "left")
        .fillna(0)
        .withColumn("as_of_date", as_of)
        .drop("customer_key")
    )

    # Note: dim_customer is NOT in this loop. It is an SCD Type 2 dimension and is
    # merged (not dropped/recreated) by scd2_merge_dim_customer above, so its
    # historical versions survive across runs.
    table_ddls: Dict[str, Tuple[str, object]] = {
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
