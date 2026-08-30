"""
the shared config/utility module that every batch Spark job imports — 
it doesn't run a pipeline step itself, 
it answers two questions for every job that does: "where do I read/write data?" 
and "how do I get a working Spark session?"

Before migration - ran every job in one container against local disk, so each script could
hardcode `BASE_DIR / "silver_delta"` and hold its own copy of the Spark builder.
After migration -  runs each Airflow task in a separate pod against GCS, which breaks both
habits:

Locations are environment-driven and default to Part 1's local layout, so the
jobs still run unchanged on a laptop with no GCS involved.
"""

from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

# LAKEHOUSE_ROOT is defined in the Airflow environment and points to a GCS bucket. If not set, the jobs run locally and write locally. 
LAKEHOUSE_ROOT = (os.getenv("LAKEHOUSE_ROOT") or "").rstrip("/")

if LAKEHOUSE_ROOT:
    _defaults = {
        "bronze": f"{LAKEHOUSE_ROOT}/bronze/offline",
        "silver_staging": f"{LAKEHOUSE_ROOT}/silver/staging",
        "silver_trusted": f"{LAKEHOUSE_ROOT}/silver/trusted",
        "reports": f"{LAKEHOUSE_ROOT}/reports",
        "feast": f"{LAKEHOUSE_ROOT}/feast",
        "delta": f"{LAKEHOUSE_ROOT}/delta/gold",
    }
else:
    _defaults = {
        "bronze": str(BASE_DIR / "generated_insurance_data" / "offline"),
        "silver_staging": str(BASE_DIR / "silver_delta_staging"),
        "silver_trusted": str(BASE_DIR / "silver_delta"),
        "reports": str(BASE_DIR / "reports"),
        "feast": str(BASE_DIR / "feast_offline"),
        "delta": str(BASE_DIR / "delta_gold"),
    }

BRONZE_ROOT = _defaults["bronze"].rstrip("/")
SILVER_STAGING_ROOT = _defaults["silver_staging"].rstrip("/")
SILVER_TRUSTED_ROOT = _defaults["silver_trusted"].rstrip("/")
REPORTS_ROOT = _defaults["reports"].rstrip("/")

# Two separate destinations for the same Gold data, serving different readers.

# plain Parquet → read by Feast, at feature-serving/retrieval time (both offline historical retrieval for training sets, and materialized into Redis for online serving
FEAST_ROOT = _defaults["feast"].rstrip("/")
# a real Delta table (with its transaction log) → read by training pipeline, not Feast. 
# The whole point of Delta is versioning. Every time the Gold-to-storage export job runs, it writes a new Delta commit, and Delta keeps every prior version accessible via versionAsOf
DELTA_ROOT = _defaults["delta"].rstrip("/")

SILVER_TABLES = ["policyholders", "policies", "claims", "payments"]

GCP_PROJECT = os.getenv("GCP_PROJECT", "")


SPARK_SCRATCH_DIR = os.getenv("SPARK_SCRATCH_DIR", str(BASE_DIR))
SPARK_WAREHOUSE_DIR = os.getenv("SPARK_WAREHOUSE_DIR", f"{SPARK_SCRATCH_DIR.rstrip('/')}/spark-warehouse")

# Baked into the Airflow image (see dockerfile.airflow). Resolving the connector
# from Maven at job start would mean every task pod downloads it again, and would
# fail outright the moment the cluster has no egress.
GCS_CONNECTOR_JAR = "/opt/spark-jars/gcs-connector-hadoop3-shaded.jar"

GCS_CONNECTOR_PACKAGE = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.30"


def is_remote(uri: str) -> bool:
    """True for object-storage URIs, which need different filesystem handling."""
    return uri.startswith("gs://")


def join(root: str, *parts: str) -> str:
    """Join URI segments. Plain string work."""
    cleaned = [str(p).strip("/") for p in parts if str(p) != ""]
    return "/".join([root.rstrip("/"), *cleaned]) if cleaned else root


def bronze_path(name: str = "") -> str:
    return join(BRONZE_ROOT, name)


def silver_staging_path(table: str = "") -> str:
    return join(SILVER_STAGING_ROOT, table)


def silver_trusted_path(table: str = "") -> str:
    return join(SILVER_TRUSTED_ROOT, table)


def report_path(filename: str = "") -> str:
    return join(REPORTS_ROOT, filename)


def feast_path(table: str = "") -> str:
    """Parquet source Feast reads for offline (historical) retrieval."""
    return join(FEAST_ROOT, table)


def delta_path(table: str = "") -> str:
    """Versioned Delta snapshot the training pipeline pins with versionAsOf."""
    return join(DELTA_ROOT, table)


def create_spark_session(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """
    Build the Spark session every batch job shares.

    Still `local[*]`, exactly as Part 1: one pod per Airflow task running embedded
    Spark. The dataset is ~30k rows, so a driver/executor split would add moving
    parts without buying throughput. The AQE and skew-join settings are Part 1's
    and are kept verbatim — the generator's Quebec skew is what motivated them,
    and that has not changed.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Delta Lake.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Part 1's tuning for the intentional data-quality problems.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.autoBroadcastJoinThreshold", "20MB")
        # Keep managed-table data and the Derby metastore off the working
        # directory, which is not reliably writable in a container.
        .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={SPARK_SCRATCH_DIR}",
        )
    )

    remote = any(
        is_remote(p)
        for p in (
            BRONZE_ROOT,
            SILVER_STAGING_ROOT,
            SILVER_TRUSTED_ROOT,
            REPORTS_ROOT,
            FEAST_ROOT,
            DELTA_ROOT,
        )
    )
    extra_packages: list[str] = []

    if remote:
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
           
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        )
        if GCP_PROJECT:
            builder = builder.config("spark.hadoop.fs.gs.project.id", GCP_PROJECT)
        if os.path.exists(GCS_CONNECTOR_JAR):
            builder = builder.config("spark.jars", GCS_CONNECTOR_JAR)
        else:
            extra_packages.append(GCS_CONNECTOR_PACKAGE)
    else:
        # Local-only nicety from Part 1: RawLocalFileSystem suppresses the hidden
        # .crc sidecar Spark otherwise drops beside every file. Meaningless
        # against GCS, and setting it there would override the connector.
        builder = (
            builder
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
        )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()


# ---------------------------------------------------------------------------
# Filesystem operations
#
# Routed through Hadoop's FileSystem API rather than `os`/`shutil` so one code
# path covers local disk and GCS. This is also why these take a SparkSession —
# the Hadoop configuration carrying the GCS credentials lives on it.
# ---------------------------------------------------------------------------

def _fs_and_path(spark: SparkSession, uri: str):
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem, hadoop_path


def path_exists(spark: SparkSession, uri: str) -> bool:
    filesystem, hadoop_path = _fs_and_path(spark, uri)
    return filesystem.exists(hadoop_path)


def delete_path(spark: SparkSession, uri: str) -> bool:
    """Recursively delete a path. Returns False when it was not there."""
    filesystem, hadoop_path = _fs_and_path(spark, uri)
    if not filesystem.exists(hadoop_path):
        return False
    return filesystem.delete(hadoop_path, True)


def list_children(spark: SparkSession, uri: str) -> list[str]:
    filesystem, hadoop_path = _fs_and_path(spark, uri)
    if not filesystem.exists(hadoop_path):
        return []
    return [status.getPath().toString() for status in filesystem.listStatus(hadoop_path)]


def write_text(spark: SparkSession, uri: str, text: str) -> None:
    """
    Write a small text file (a quality report) to local disk or GCS.

    Reports have to be readable by a *later* Airflow task, which is a different
    pod with a different filesystem — so this cannot be a plain open()/write().
    """
    filesystem, hadoop_path = _fs_and_path(spark, uri)
    stream = filesystem.create(hadoop_path, True)  # overwrite
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark: SparkSession, uri: str) -> str:
    filesystem, hadoop_path = _fs_and_path(spark, uri)
    stream = filesystem.open(hadoop_path)
    try:
        data = bytearray()
        while True:
            byte = stream.read()
            if byte == -1:
                break
            data.append(byte)
        return data.decode("utf-8")
    finally:
        stream.close()


def describe_locations() -> str:
    """Log line so every task pod records which storage it actually used."""
    return (
        f"bronze={BRONZE_ROOT} "
        f"silver_staging={SILVER_STAGING_ROOT} "
        f"silver_trusted={SILVER_TRUSTED_ROOT} "
        f"reports={REPORTS_ROOT} "
        f"feast={FEAST_ROOT} "
        f"delta={DELTA_ROOT}"
    )
