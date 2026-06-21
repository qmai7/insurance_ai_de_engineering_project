"""
Publish DataHub dataset metadata and lineage for the insurance pipeline.

Lineage:
Bronze raw files
    -> Silver Delta Lake tables
    -> Gold ClickHouse tables
    -> Feature table

This script is called by the final Airflow task after Gold quality checks pass.
"""

from __future__ import annotations

import os
import sys

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from datahub.metadata.schema_classes import (
    AzkabanJobTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)


DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
ENV = os.getenv("DATAHUB_ENV", "PROD")

# Must match the Airflow DAG in dags/insurance_batch_pipeline.py so the lineage
# graph in DataHub lines up with what actually runs.
AIRFLOW_DAG_ID = "insurance_batch_bronze_silver_gold"
AIRFLOW_UI_URL = os.getenv("AIRFLOW_UI_URL", "http://localhost:8080")

# the naming helper
def dataset_urn(platform: str, name: str) -> str: 
    return make_dataset_urn(platform=platform, name=name, env=ENV)

# register a dataset's descriptive metadata (name, description) in DataHub.
def emit_dataset_properties(emitter: DatahubRestEmitter,urn: str,name: str,description: str,) -> None: 
    aspect = DatasetPropertiesClass(name=name,description=description,)

    mcp = MetadataChangeProposalWrapper(entityUrn=urn,aspect=aspect)
    emitter.emit(mcp)
    print(f"Published dataset properties: {urn}")

# drawing the arrows between datasets
def emit_lineage(emitter: DatahubRestEmitter,downstream_urn: str,upstream_urns: list[str],) -> None:
    upstreams = [
        UpstreamClass(
            dataset=upstream_urn,
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
        for upstream_urn in upstream_urns
    ]

    aspect = UpstreamLineageClass(upstreams=upstreams)

    mcp = MetadataChangeProposalWrapper(entityUrn=downstream_urn,aspect=aspect)

    emitter.emit(mcp)
    print(f"Published lineage: {upstream_urns} -> {downstream_urn}")

# registering the DAG itself 
def emit_data_flow(emitter: DatahubRestEmitter, flow_urn: str, name: str, description: str) -> None:
    """Register the Airflow DAG itself so 'Airflow' shows up as an orchestrator/platform."""
    aspect = DataFlowInfoClass(
        name=name,
        description=description,
        externalUrl=AIRFLOW_UI_URL,
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=flow_urn, aspect=aspect))
    print(f"Published data flow: {flow_urn}")


def emit_data_job(
    emitter: DatahubRestEmitter,
    flow_urn: str,
    task_id: str,
    description: str,
    input_datasets: list[str] | None = None,
    output_datasets: list[str] | None = None,
    upstream_jobs: list[str] | None = None,
) -> str:
    """
    Register one Airflow task (a 'block' in the DAG) and connect it to the datasets
    it reads and writes. This is what lets DataHub answer 'which task produced this
    table' and draw task-level lineage, not just dataset-to-dataset edges.
    """
    job_urn = make_data_job_urn_with_flow(flow_urn, task_id)

    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=task_id,
                type=AzkabanJobTypeClass.COMMAND,
                description=description,
                flowUrn=flow_urn,
                externalUrl=AIRFLOW_UI_URL,
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_datasets or [],
                outputDatasets=output_datasets or [],
                inputDatajobs=upstream_jobs or [],
            ),
        )
    )
    print(f"Published data job: {job_urn}")
    return job_urn


def main() -> None:
    print(f"Connecting to DataHub GMS: {DATAHUB_GMS_URL}")

    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)

    # Bronze/source datasets.
    bronze_policyholders = dataset_urn("file", "bronze.policyholders")
    bronze_policies = dataset_urn("file", "bronze.policies")
    bronze_claims = dataset_urn("file", "bronze.claims")
    bronze_payments = dataset_urn("file", "bronze.payments")

    # Silver Delta datasets.
    silver_policyholders = dataset_urn("delta-lake", "silver_delta.policyholders")
    silver_policies = dataset_urn("delta-lake", "silver_delta.policies")
    silver_claims = dataset_urn("delta-lake", "silver_delta.claims")
    silver_payments = dataset_urn("delta-lake", "silver_delta.payments")

    # Gold ClickHouse datasets.
    dim_customer = dataset_urn("clickhouse", "gold_insurance.dim_customer")
    dim_policy = dataset_urn("clickhouse", "gold_insurance.dim_policy")
    dim_date = dataset_urn("clickhouse", "gold_insurance.dim_date")
    fact_claims = dataset_urn("clickhouse", "gold_insurance.fact_claims")
    fact_payment_attempts = dataset_urn("clickhouse", "gold_insurance.fact_payment_attempts")
    obt_claims_enriched = dataset_urn("clickhouse","gold_insurance.obt_claims_enriched")
    feat_customer_90d = dataset_urn("clickhouse", "gold_insurance.feat_customer_90d")

    datasets = {
        bronze_policyholders: (
            "Bronze policyholders",
            "Raw generated policyholder/customer source data.",
        ),
        bronze_policies: (
            "Bronze policies",
            "Raw generated insurance policy source data.",
        ),
        bronze_claims: (
            "Bronze claims",
            "Raw generated insurance claims source data.",
        ),
        bronze_payments: (
            "Bronze payments",
            "Raw generated insurance payment source data.",
        ),
        silver_policyholders: (
            "Silver policyholders",
            "Cleaned, deduplicated, quality-gated Delta table for policyholders.",
        ),
        silver_policies: (
            "Silver policies",
            "Cleaned, deduplicated, quality-gated Delta table for policies.",
        ),
        silver_claims: (
            "Silver claims",
            "Cleaned, deduplicated, quality-gated Delta table for claims.",
        ),
        silver_payments: (
            "Silver payments",
            "Cleaned, deduplicated, quality-gated Delta table for payments.",
        ),
        dim_customer: (
            "Gold dim_customer",
            "ClickHouse customer dimension table.",
        ),
        dim_policy: (
            "Gold dim_policy",
            "ClickHouse policy dimension table.",
        ),
        dim_date: (
            "Gold dim_date",
            "ClickHouse date dimension table.",
        ),
        fact_claims: (
            "Gold fact_claims",
            "ClickHouse claims fact table.",
        ),
        fact_payment_attempts: (
            "Gold fact_payment_attempts",
            "ClickHouse payment attempts fact table.",
        ),
        obt_claims_enriched: (
            "Gold obt_claims_enriched",
            "Transaction-grain (one per claim) denormalized ClickHouse table for claim/loss BI.",
        ),
        feat_customer_90d: (
            "Gold feat_customer_90d",
            "Offline customer feature table for 90-day claim and payment behavior.",
        ),
    }

    for urn, (name, description) in datasets.items():
        emit_dataset_properties(emitter, urn, name, description)

    # Bronze -> Silver lineage.
    emit_lineage(emitter, silver_policyholders, [bronze_policyholders])
    emit_lineage(emitter, silver_policies, [bronze_policies])
    emit_lineage(emitter, silver_claims, [bronze_claims])
    emit_lineage(emitter, silver_payments, [bronze_payments])

    # Silver -> Gold lineage.
    emit_lineage(emitter, dim_customer, [silver_policyholders])
    emit_lineage(emitter, dim_policy, [silver_policies, silver_policyholders])
    emit_lineage(
        emitter,
        dim_date,
        [silver_policies, silver_claims, silver_payments],
    )
    emit_lineage(emitter, fact_claims, [silver_claims, silver_policies])
    emit_lineage(emitter, fact_payment_attempts, [silver_payments, silver_policies])

    # Gold -> OBT / features lineage.
    emit_lineage(
        emitter,
        obt_claims_enriched,
        [silver_claims, silver_policies, silver_policyholders],
    )
    emit_lineage(
        emitter,
        feat_customer_90d,
        [dim_customer, dim_policy, fact_claims, fact_payment_attempts, dim_date],
    )

    # ------------------------------------------------------------------
    # Airflow orchestration lineage.
    #
    # The dataset->dataset edges above show how data flows, but they do not tell
    # DataHub that Airflow runs this pipeline or which task produces which table.
    # Registering the DAG as a DataFlow (orchestrator='airflow') makes Airflow
    # appear as a platform, and each DataJob (task) is connected to the datasets
    # it reads/writes so you can trace a table back to the block that built it.
    # The task chain mirrors dags/insurance_batch_pipeline.py exactly.
    # ------------------------------------------------------------------
    bronze_datasets = [bronze_policyholders, bronze_policies, bronze_claims, bronze_payments]
    silver_datasets = [silver_policyholders, silver_policies, silver_claims, silver_payments]
    gold_datasets = [
        dim_customer,
        dim_policy,
        dim_date,
        fact_claims,
        fact_payment_attempts,
        obt_claims_enriched,
        feat_customer_90d,
    ]

    flow_urn = make_data_flow_urn("airflow", AIRFLOW_DAG_ID, cluster=ENV)
    emit_data_flow(
        emitter,
        flow_urn,
        name=AIRFLOW_DAG_ID,
        description="Batch pipeline: Bronze files -> Silver Delta -> Gold ClickHouse, with quality gates.",
    )

    validate_job = emit_data_job(
        emitter,
        flow_urn,
        "validate_bronze_inputs_exist",
        "Checks that the manually generated Bronze source files exist before ingestion.",
        input_datasets=bronze_datasets,
    )
    transform_job = emit_data_job(
        emitter,
        flow_urn,
        "transform_silver_delta",
        "Spark job that cleans/casts/deduplicates Bronze and produces the Silver Delta tables.",
        input_datasets=bronze_datasets,
        output_datasets=silver_datasets,
        upstream_jobs=[validate_job],
    )
    silver_gate_job = emit_data_job(
        emitter,
        flow_urn,
        "run_silver_quality_gate",
        "Validates the Silver tables; stops the pipeline before Gold if checks fail.",
        input_datasets=silver_datasets,
        upstream_jobs=[transform_job],
    )
    publish_silver_job = emit_data_job(
        emitter,
        flow_urn,
        "publish_silver_delta",
        "Promotes the validated Silver staging tables to the trusted Silver Delta layer.",
        input_datasets=silver_datasets,
        upstream_jobs=[silver_gate_job],
    )
    build_gold_job = emit_data_job(
        emitter,
        flow_urn,
        "build_gold_clickhouse",
        "Spark job that models Silver into ClickHouse Gold dims, facts, OBT, and features.",
        input_datasets=silver_datasets,
        output_datasets=gold_datasets,
        upstream_jobs=[publish_silver_job],
    )
    gold_gate_job = emit_data_job(
        emitter,
        flow_urn,
        "run_gold_quality_gate",
        "Validates the Gold ClickHouse tables and persists results to quality_check_results.",
        input_datasets=gold_datasets,
        upstream_jobs=[build_gold_job],
    )
    emit_data_job(
        emitter,
        flow_urn,
        "publish_datahub_lineage_stub",
        "Publishes dataset, task, and lineage metadata to DataHub (this script).",
        upstream_jobs=[gold_gate_job],
    )

    print("DataHub lineage publishing completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"DataHub lineage publishing failed: {exc}", file=sys.stderr)
        sys.exit(1)