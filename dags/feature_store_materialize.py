"""
Materialize Pipeline — the batch feature path (CLAUDE.md §3, pipeline 1).

Gold (ClickHouse) -> GCS Parquet -> Feast registry -> Redis online store.

Two images, on purpose. The export step needs Spark, Delta, and the GCS
connector; the Feast steps need feast[redis,gcp], which resolves to numpy 2.2 and
pandas 2.3 and would break PySpark 3.5.1's pandas conversion. Keeping them in
separate images means neither dependency set can damage the other, so the Feast
tasks run as KubernetesPodOperator against the dedicated Feast image while the
export runs in the Airflow image itself.

This is pipeline 1 of the three in §3. Jobs 1 and 2 push *streaming* features into
the offline and online stores and are not buildable yet — they consume Flink's
output, and the Kafka/Flink work is deferred.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

FEAST_IMAGE = (
    "northamerica-northeast1-docker.pkg.dev/aide-playground/insurance-images/feast:0.1.0"
)
NAMESPACE = "data-ns"

# The Feast tasks reach GCS and Redis, so they run as the Workload-Identity
# annotated KSA rather than the namespace default.
SERVICE_ACCOUNT = "airflow"

DEFAULT_ARGS = {
    "owner": "quan",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# Modest: Feast here moves 12,700 rows, and Autopilot bills what is requested.
FEAST_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "2Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
)


def feast_task(task_id: str, command: str) -> KubernetesPodOperator:
    """A Feast CLI invocation in its own pod."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=FEAST_IMAGE,
        service_account_name=SERVICE_ACCOUNT,
        cmds=["sh", "-c"],
        arguments=[command],
        container_resources=FEAST_RESOURCES,
        # Task logs are otherwise lost with the pod, so pull them into the
        # Airflow task log while the pod is still alive.
        get_logs=True,
        on_finish_action="delete_pod",
        in_cluster=True,
        # Without this the operator reuses a prior pod for the same task and
        # skips the run entirely on a retry.
        reattach_on_restart=False,
    )


with DAG(
    dag_id="feature_store_materialize",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 11, 1),
    # Manual trigger, matching the batch DAG. Feast's incremental materialization
    # is driven by an end timestamp rather than a schedule, so nothing here needs
    # a timer to be correct.
    schedule_interval=None,
    catchup=False,
    tags=["insurance", "feature-store", "feast", "batch"],
) as dag:

    # Runs in the Airflow image: needs Spark to read ClickHouse into Parquet and
    # to write the parallel Delta snapshot the training pipeline versions.
    export_gold = BashOperator(
        task_id="export_gold_to_gcs",
        bash_command="cd /opt/airflow && python jobs/export_gold_to_feast.py",
    )

    # Registers entities and feature views into the GCS registry. Idempotent, and
    # cheap enough to run every time so the registry can never lag the code.
    feast_apply = feast_task(
        "feast_apply",
        "cd /feature_store && feast apply",
    )

    # `materialize-incremental` loads everything from the last materialization up
    # to the end timestamp, which is what makes repeated runs cheap. The timestamp
    # is computed in the pod at run time.
    feast_materialize = feast_task(
        "feast_materialize_incremental",
        "cd /feature_store && feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)",
    )

    # Proves Redis actually answers, rather than trusting that materialize
    # reported success. A silently empty online store looks identical to a
    # working one until the prediction API starts returning nothing.
    verify_online = feast_task(
        "verify_online_store",
        "cd /feature_store && python verify_online.py",
    )

    export_gold >> feast_apply >> feast_materialize >> verify_online
