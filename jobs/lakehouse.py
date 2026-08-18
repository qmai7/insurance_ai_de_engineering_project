"""
Shared storage locations and Spark construction for the batch pipeline.

Part 1 ran every job in one container against local disk, so each script could
hardcode `BASE_DIR / "silver_delta"` and hold its own copy of the Spark builder.
Part 2 runs each Airflow task in a separate pod against GCS, which breaks both
habits:

- Local paths do not survive a pod boundary. Anything one task writes for another
  task to read has to live in object storage.
- `pathlib.Path` cannot represent a GCS URI. `Path("gs://b/x")` collapses the
  double slash to `gs:/b/x`, so paths are built as plain strings here.
- Reading and writing GCS needs the connector jar and auth wired into every
  session, which is not worth repeating five times.

Locations are environment-driven and default to Part 1's local layout, so the
jobs still run unchanged on a laptop with no GCS involved.
"""

from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

# A single root is the convenient way to point the whole pipeline at a bucket:
#   LAKEHOUSE_ROOT=gs://aide-playground-lakehouse
# Individual layers can still be overridden one at a time (see below), which is
# what makes it possible to read Bronze from the bucket while writing Silver
# somewhere else during debugging.
LAKEHOUSE_ROOT = (os.getenv("LAKEHOUSE_ROOT") or "").rstrip("/")

if LAKEHOUSE_ROOT:
    _defaults = {
        "bronze": f"{LAKEHOUSE_ROOT}/bronze/offline",
        "silver_staging": f"{LAKEHOUSE_ROOT}/silver/staging",
        "silver_trusted": f"{LAKEHOUSE_ROOT}/silver/trusted",
        "reports": f"{LAKEHOUSE_ROOT}/reports",
    }
else:
    _defaults = {
        "bronze": str(BASE_DIR / "generated_insurance_data" / "offline"),
        "silver_staging": str(BASE_DIR / "silver_delta_staging"),
        "silver_trusted": str(BASE_DIR / "silver_delta"),
        "reports": str(BASE_DIR / "reports"),
    }

BRONZE_ROOT = (os.getenv("BRONZE_ROOT") or _defaults["bronze"]).rstrip("/")
SILVER_STAGING_ROOT = (os.getenv("SILVER_STAGING_ROOT") or _defaults["silver_staging"]).rstrip("/")
SILVER_TRUSTED_ROOT = (os.getenv("SILVER_TRUSTED_ROOT") or _defaults["silver_trusted"]).rstrip("/")
REPORTS_ROOT = (os.getenv("REPORTS_ROOT") or _defaults["reports"]).rstrip("/")

SILVER_TABLES = ["policyholders", "policies", "claims", "payments"]

GCP_PROJECT = os.getenv("GCP_PROJECT", "")

# Pod-local scratch for Spark's managed-table warehouse and the Derby metastore.
#
# Both default to the current working directory, which is fine when a laptop runs
# the job from the repo root but not inside a container where the working
# directory may be read-only. This is genuinely temporary state — the gold job
# writes bucketed tables here and consumes them in the same run — so a path that
# vanishes with the pod is correct, not a limitation.
SPARK_SCRATCH_DIR = os.getenv("SPARK_SCRATCH_DIR", str(BASE_DIR))
SPARK_WAREHOUSE_DIR = os.getenv("SPARK_WAREHOUSE_DIR", f"{SPARK_SCRATCH_DIR.rstrip('/')}/spark-warehouse")

# Baked into the Airflow image (see dockerfile.airflow). Resolving the connector
# from Maven at job start would mean every task pod downloads it again, and would
# fail outright the moment the cluster has no egress.
GCS_CONNECTOR_JAR = os.getenv("GCS_CONNECTOR_JAR", "/opt/spark-jars/gcs-connector-hadoop3-shaded.jar")

# Fallback when that jar is absent — a laptop testing against the bucket rather
# than a task pod. Resolved from Maven at startup, which is slow and needs
# egress, so it is a convenience path and not what runs in the cluster.
# 3.0.x pairs with Hadoop 3.3.x, which is what Spark 3.5.1 in the image bundles.
# The connector is tightly coupled to Hadoop's internals: 3.1.x targets Hadoop
# 3.4 and dies against 3.3 with NoSuchMethodError on VectoredReadUtils, so this
# version travels with the Spark version and is not a free upgrade.
GCS_CONNECTOR_PACKAGE = os.getenv(
    "GCS_CONNECTOR_PACKAGE", "com.google.cloud.bigdataoss:gcs-connector:3.0.19"
)


def is_remote(uri: str) -> bool:
    """True for object-storage URIs, which need different filesystem handling."""
    return uri.startswith("gs://")


def join(root: str, *parts: str) -> str:
    """Join URI segments. Plain string work — see the pathlib caveat above."""
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
        for p in (BRONZE_ROOT, SILVER_STAGING_ROOT, SILVER_TRUSTED_ROOT, REPORTS_ROOT)
    )
    extra_packages: list[str] = []

    if remote:
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            # Auth via the full Application Default Credentials chain, which
            # resolves to the GKE metadata server under Workload Identity in the
            # cluster and to gcloud's ADC file on a developer laptop. No key file
            # exists in either case.
            #
            # This needs connector 3.x. The 2.2.x line does not understand
            # fs.gs.auth.type and silently authenticates against the GCE
            # metadata server only, which fails anywhere outside GCP.
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
        f"reports={REPORTS_ROOT}"
    )
