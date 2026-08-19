"""
Export Gold feature tables out of ClickHouse into GCS.

Writes each table twice, to two formats, for two different readers:

  feast/<table>/   plain Parquet — what Feast's FileSource reads for historical
                   retrieval and what `feast materialize` pushes into Redis.
  delta/gold/<t>/  the same rows as a Delta table, read only by the training
                   pipeline. Delta versions every write in its transaction log,
                   so a training run can pin `versionAsOf` and record that number
                   as an MLflow tag (§7). Keeping it parallel to the Parquet
                   export, rather than replacing it, avoids having to answer
                   whether Feast reads Delta.

Why export at all, instead of pointing Feast at ClickHouse: the community
ClickHouse offline store for Feast is unstable, so CLAUDE.md deliberately routes
through GCS Parquet instead.

ClickHouse is read with clickhouse-connect into pandas rather than over Spark
JDBC. The volumes are small (10k customers, 2.7k claims) and it avoids shipping
and configuring a JDBC driver for no gain.
"""

from __future__ import annotations

import os

import clickhouse_connect
import lakehouse
import pandas as pd
from pyspark.sql import functions as F

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")

# Feast requires an event-timestamp column on every source so it can do
# point-in-time correct joins — pick the wrong column here and training silently
# leaks future information. Each export names the Gold column that carries the
# feature's real as-of time.
EXPORTS = [
    {
        "table": "feat_customer_90d",
        # Customer aggregates over a trailing 90-day window.
        "timestamp_from": "as_of_date",
        "entity": "customer_id",
    },
    {
        "table": "obt_claims_enriched",
        # Claim-level attributes. claim_date is when the claim actually happened,
        # which is the only honest as-of time for a claim's own attributes.
        "timestamp_from": "claim_date",
        "entity": "claim_id",
    },
]


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def to_spark_friendly(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pandas extension dtypes to the plain ones Spark can infer.

    clickhouse-connect returns modern pandas dtypes — `string[python]` holding
    pd.NA, unsigned `uint8`/`uint16`, and second-precision `datetime64[s]`.
    PySpark 3.5.1 does not enable Arrow for pandas conversion by default, so it
    falls back to sampling rows and inferring types, and those dtypes make it
    guess wrong:

        PySparkTypeError: [CANNOT_MERGE_TYPE] Can not merge type
        `StringType` and `StructType`

    Normalising up front is preferable to enabling Arrow, because it keeps the
    conversion explicit and does not make correctness depend on a pyarrow version
    matching Spark's expectations.
    """
    out = pdf.copy()
    for col in out.columns:
        series = out[col]
        has_nulls = series.isna().any()

        if pd.api.types.is_bool_dtype(series):
            out[col] = series.astype(bool)
        elif pd.api.types.is_integer_dtype(series):
            # Spark has no unsigned types. A nullable integer cannot become int64
            # without inventing a value for the nulls, so it widens to float
            # instead — losing nothing at these magnitudes.
            out[col] = series.astype("float64") if has_nulls else series.astype("int64")
        elif pd.api.types.is_float_dtype(series):
            out[col] = series.astype("float64")
        elif pd.api.types.is_datetime64_any_dtype(series):
            # Spark's TimestampType is microsecond-based; datetime64[s] is not
            # something its inference path recognises.
            out[col] = series.astype("datetime64[ns]")
        else:
            # Strings and anything else: plain object with real None, because
            # Spark does not understand pd.NA.
            out[col] = series.astype(object).where(series.notna(), None)
    return out


def export_table(spark, client, spec: dict) -> int:
    table = spec["table"]
    pdf = client.query_df(f"SELECT * FROM {CLICKHOUSE_DATABASE}.{table}")
    row_count = len(pdf)
    if row_count == 0:
        raise ValueError(f"{table} is empty; run the batch DAG before exporting")

    df = spark.createDataFrame(to_spark_friendly(pdf))

    # Feast wants a real timestamp, not a date. The Gold columns are DATE, which
    # Feast will reject or silently mishandle in a point-in-time join.
    df = df.withColumn("event_timestamp", F.to_timestamp(F.col(spec["timestamp_from"])))

    # Feast uses created_timestamp to break ties when two rows share an
    # event_timestamp. Every row in a given export is produced by the same run,
    # so a single run-level stamp is the truthful value.
    df = df.withColumn("created_timestamp", F.current_timestamp())

    parquet_target = lakehouse.feast_path(table)
    delta_target = lakehouse.delta_path(table)

    # Overwrite, not append: Gold itself is rebuilt wholesale by the batch DAG, so
    # appending would stack duplicate snapshots of identical rows and corrupt
    # point-in-time joins.
    df.write.mode("overwrite").parquet(parquet_target)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(delta_target)
    )

    print(f"{table}: rows={row_count} entity={spec['entity']}")
    print(f"  parquet -> {parquet_target}")
    print(f"  delta   -> {delta_target}")
    return row_count


def main() -> None:
    spark = lakehouse.create_spark_session("export_gold_to_feast")
    print(f"storage: {lakehouse.describe_locations()}")
    print(f"clickhouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")

    client = clickhouse_client()
    total = 0
    for spec in EXPORTS:
        total += export_table(spark, client, spec)

    # Delta version numbers are the point of the Delta copy, so surface them —
    # this is what a training run pins and logs to MLflow.
    print("\nDelta versions available for training:")
    for spec in EXPORTS:
        target = lakehouse.delta_path(spec["table"])
        version = (
            spark.sql(f"DESCRIBE HISTORY delta.`{target}`")
            .agg(F.max("version"))
            .collect()[0][0]
        )
        print(f"  {spec['table']}: latest version={version}")

    spark.stop()
    print(f"\nExported {len(EXPORTS)} tables, {total} rows total.")


if __name__ == "__main__":
    main()
