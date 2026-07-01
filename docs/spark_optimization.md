# Spark job to handle offline data problems

## 1. Duplication,
The claim data contained 54 duplicate claim IDs (3.92% duplicate rate). We use the `deduplicate_by_key` function below, which uses the window function "partitionBy" to group rows with the same "claim_id," then "orderBy" "claim_date" (sorting by newest date first). We then assign sequential numbers after sorting (using the _rn column), keep only the row where "_rn" = 1, and drop "rn". 

```python
def deduplicate_by_key(df, key_column: str, order_column: str):
    """Keep the newest row per business key using a deterministic window rule."""
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
```

## 2. Schema evolution: 
Before a fictive date `schema_change_date` configured in data generator, the source system does not have `risk_segment` attribute in `policyholders.parquet`. Thus, the Silver layer uses `mergeSchema` to reconcile historical and current schemas, filling `risk_segment` with null for legacy records. 

```python
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
            .parquet(str(BRONZE_DIR / "policyholders"))
        )

    return spark.read.parquet(str(BRONZE_DIR / f"{table_name}.parquet"))
```

## 3. High cardinality:
The Gold layer handles high-cardinality customer-level processing using bucketing on `customer_key`. Since the feature table aggregates claim and payment activity by customer, Spark would normally shuffle records so all rows for each customer are colocated. By writing reusable bucketed fact tables on `customer_key`, rows for the same customer are pre-organized into a fixed number of buckets, reducing shuffle cost for repeated customer-level aggregations and joins.

```python
 BUCKETS = 8

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
```
