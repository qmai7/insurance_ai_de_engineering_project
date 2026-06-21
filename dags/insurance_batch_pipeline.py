"""
Airflow DAG for the batch part of the insurance data/AI engineering project.

The synthetic data generator is intentionally NOT part of this DAG.
It should be run manually once before the pipeline starts, because it represents
source-system data that already exists before ingestion/transformation begins.

Production-style flow:
1. Validate that manually generated Bronze/source files exist.
2. Transform Bronze files into Silver_Delta_Staging tables.
3. Run Silver quality checks as a gate.
4. Publish Silver Delta tables only if quality checks pass
4. Build Gold warehouse tables in ClickHouse only if Silver passes.
5. Run Gold quality checks.
6. Publish a DataHub lineage.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "quan",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="insurance_batch_bronze_silver_gold",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 11, 1),
    # Manual-trigger only: the DAG never runs on a timer; it runs when you
    # trigger it from the Airflow UI/CLI. (Was "*/30 * * * *".)
    schedule_interval=None,
    catchup=False,
    tags=["insurance", "batch", "silver", "gold", "sla"],
) as dag:

    validate_bronze_inputs = BashOperator(
        task_id="validate_bronze_inputs_exist",
        bash_command="""
        set -e
        test -f /opt/airflow/generated_insurance_data/offline/policyholders.parquet
        test -f /opt/airflow/generated_insurance_data/offline/policies.parquet
        test -f /opt/airflow/generated_insurance_data/offline/claims.parquet
        test -f /opt/airflow/generated_insurance_data/offline/payments.parquet
        echo 'Bronze/source files exist. Continuing pipeline.'
        """,
    )

    silver_delta = BashOperator(
        task_id="transform_silver_delta",
        bash_command="cd /opt/airflow && python jobs/silver_cleaning_delta.py",
    )

    silver_quality_gate = BashOperator(
        task_id="run_silver_quality_gate",
        bash_command="cd /opt/airflow && python jobs/silver_quality_checks.py",
    )

    publish_silver_delta = BashOperator(
        task_id="publish_silver_delta",
        bash_command="cd /opt/airflow && python jobs/publish_silver_delta.py",
    )

    gold_clickhouse = BashOperator(
        task_id="build_gold_clickhouse",
        bash_command="cd /opt/airflow && python jobs/gold_clickhouse.py",
    )

    gold_quality_gate = BashOperator(
        task_id="run_gold_quality_gate",
        bash_command="cd /opt/airflow && python jobs/quality_checks_clickhouse.py",
    )

    publish_lineage = BashOperator(
        task_id="publish_datahub_lineage_stub",
        bash_command="cd /opt/airflow && python jobs/publish_datahub_lineage.py",
    )

    validate_bronze_inputs >> silver_delta >> silver_quality_gate >> publish_silver_delta >> gold_clickhouse >> gold_quality_gate >> publish_lineage