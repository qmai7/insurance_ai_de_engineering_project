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

import json
import os
import sys
import time
from pathlib import Path

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    AzkabanJobTypeClass,
    CustomAssertionInfoClass,
    DataContractPropertiesClass,
    DataContractStateClass,
    DataContractStatusClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataQualityContractClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
    FreshnessContractClass,
    FreshnessCronScheduleClass,
    RowCountTotalClass,
    UpstreamClass,
    UpstreamLineageClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)
from datahub.emitter.mce_builder import make_assertion_urn, make_schema_field_urn


DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
ENV = os.getenv("DATAHUB_ENV", "PROD")

# Matches silver_quality_checks.py's BASE_DIR (parents[1] of that script, i.e.
# the project root) + "reports/silver_quality_report.json". Overridable via
# env var in case this script runs in a different container/mount.
_DEFAULT_SILVER_REPORT_PATH = str(
    Path(__file__).resolve().parents[1] / "reports" / "silver_quality_report.json"
)
SILVER_QUALITY_REPORT_PATH = os.getenv("SILVER_QUALITY_REPORT_PATH", _DEFAULT_SILVER_REPORT_PATH)

# Matches quality_checks_clickhouse.py's connection config exactly, since we're
# reading the same gold_insurance.quality_check_results table it writes.
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")


def load_gold_quality_results() -> dict:
    """
    Read the real Gold check results that quality_checks_clickhouse.py wrote
    to gold_insurance.quality_check_results. Returns
    {check_name: (status, failure_count)}.
    Falls back to an empty dict (with a warning) if ClickHouse isn't reachable
    or the table doesn't exist yet, so this script can still run without
    faking results.
    """
    try:
        import clickhouse_connect
    except ImportError:
        print("WARNING: clickhouse_connect not installed; skipping Gold validation.", file=sys.stderr)
        return {}

    try:
        ch = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
        rows = ch.query(
            f"SELECT check_name, status, failure_count "
            f"FROM {CLICKHOUSE_DATABASE}.quality_check_results"
        ).result_rows
        return {name: (status, int(failures)) for name, status, failures in rows}
    except Exception as exc:
        print(f"WARNING: could not read Gold quality results from ClickHouse: {exc}", file=sys.stderr)
        return {}


def load_silver_quality_report() -> dict:
    """
    Load the real check results written by silver_quality_checks.py.
    Returns {"checks": {name: bool}, "row_counts": {table: int}}.
    Falls back to an empty report (with a loud warning) if the file isn't
    there yet, so this script can still run rather than crash the DAG --
    but the assertions it publishes in that case are skipped, not faked.
    """
    path = Path(SILVER_QUALITY_REPORT_PATH)
    if not path.exists():
        print(
            f"WARNING: quality report not found at {path}. "
            "Skipping Silver validation/contract publishing this run.",
            file=sys.stderr,
        )
        return {"checks": {}, "row_counts": {}}
    return json.loads(path.read_text())

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


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Data validation: assertions (mirrors run_silver_quality_gate / run_gold_quality_gate)
# ---------------------------------------------------------------------------
def emit_row_count_assertion(
    emitter: DatahubRestEmitter,
    entity_urn: str,
    assertion_id: str,
    min_rows: int,
    observed_row_count: int,
) -> str:
    """Define + evaluate a 'row count > min_rows' volume assertion."""
    assertion_urn = make_assertion_urn(assertion_id)

    info = AssertionInfoClass(
        type=AssertionTypeClass.VOLUME,
        volumeAssertion=VolumeAssertionInfoClass(
            type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=entity_urn,
            rowCountTotal=RowCountTotalClass(
                operator=AssertionStdOperatorClass.GREATER_THAN,
                parameters=AssertionStdParametersClass(
                    value=AssertionStdParameterClass(
                        type=AssertionStdParameterTypeClass.NUMBER,
                        value=str(min_rows),
                    ),
                ),
            ),
        ),
        description=f"Row count for {entity_urn} must be greater than {min_rows}.",
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info))

    passed = observed_row_count > min_rows
    run_event = AssertionRunEventClass(
        timestampMillis=now_ms(),
        runId=f"{assertion_id}-{now_ms()}",
        asserteeUrn=entity_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=AssertionResultTypeClass.SUCCESS if passed else AssertionResultTypeClass.FAILURE,
            actualAggValue=observed_row_count,
        ),
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event))
    print(f"Published assertion {assertion_urn} on {entity_urn}: "
          f"{'PASSED' if passed else 'FAILED'} ({observed_row_count} rows)")
    return assertion_urn


def emit_custom_check_assertion(
    emitter: DatahubRestEmitter,
    entity_urn: str,
    assertion_id: str,
    category: str,
    description: str,
    passed: bool,
    field: str | None = None,
) -> str:
    """
    Publish a generic boolean pass/fail check as a DataHub CUSTOM assertion.

    This is the right fit for checks that don't map onto DataHub's built-in
    Volume/Freshness/Schema assertion types -- e.g. "key column is not null",
    "key column has no duplicates after dedup", or a business rule like
    "amount must be non-negative". `category` is how it will be grouped/
    labeled in the DataHub UI (e.g. "Not Null", "Uniqueness", "Business Rule").
    """
    assertion_urn = make_assertion_urn(assertion_id)

    # DataHub's customAssertion.field expects a schemaField URN, not a bare
    # column name -- GMS rejects a raw name like "customer_id" as an invalid urn.
    field_urn = make_schema_field_urn(entity_urn, field) if field else None
    info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customAssertion=CustomAssertionInfoClass(
            type=category,
            entity=entity_urn,
            field=field_urn,
        ),
        description=description,
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info))

    run_event = AssertionRunEventClass(
        timestampMillis=now_ms(),
        runId=f"{assertion_id}-{now_ms()}",
        asserteeUrn=entity_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=AssertionResultTypeClass.SUCCESS if passed else AssertionResultTypeClass.FAILURE,
        ),
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event))
    print(f"Published {category} assertion {assertion_urn} on {entity_urn}: "
          f"{'PASSED' if passed else 'FAILED'}")
    return assertion_urn


def emit_freshness_assertion(
    emitter: DatahubRestEmitter,
    entity_urn: str,
    assertion_id: str,
    max_hours_since_update: int,
    hours_since_update: float,
) -> str:
    assertion_urn = make_assertion_urn(assertion_id)

    info = AssertionInfoClass(
        type=AssertionTypeClass.FRESHNESS,
        freshnessAssertion=FreshnessAssertionInfoClass(
            type=FreshnessAssertionTypeClass.DATASET_CHANGE,
            entity=entity_urn,
            schedule=FreshnessAssertionScheduleClass(
                type=FreshnessAssertionScheduleTypeClass.CRON,
                cron=FreshnessCronScheduleClass(
                    cron="0 6 * * *",  # expected refresh by 6am daily
                    timezone="UTC",
                ),
            ),
        ),
        description=f"{entity_urn} must be refreshed within {max_hours_since_update}h of the daily batch run.",
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info))

    passed = hours_since_update <= max_hours_since_update
    run_event = AssertionRunEventClass(
        timestampMillis=now_ms(),
        runId=f"{assertion_id}-{now_ms()}",
        asserteeUrn=entity_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=AssertionResultTypeClass.SUCCESS if passed else AssertionResultTypeClass.FAILURE,
        ),
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event))
    print(f"Published freshness assertion {assertion_urn} on {entity_urn}: "
          f"{'PASSED' if passed else 'FAILED'} ({hours_since_update}h old)")
    return assertion_urn


# ---------------------------------------------------------------------------
# Data contract: bundles assertions into a public promise for one dataset
# ---------------------------------------------------------------------------
def emit_data_contract(
    emitter: DatahubRestEmitter,
    entity_urn: str,
    contract_id: str,
    quality_assertion_urns: list[str],
    freshness_assertion_urn: str | None = None,
) -> str:
    contract_urn = f"urn:li:dataContract:{contract_id}"

    properties = DataContractPropertiesClass(
        entity=entity_urn,
        freshness=[FreshnessContractClass(assertion=freshness_assertion_urn)] if freshness_assertion_urn else None,
        dataQuality=[DataQualityContractClass(assertion=urn) for urn in quality_assertion_urns],
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=contract_urn, aspect=properties))

    status = DataContractStatusClass(state=DataContractStateClass.ACTIVE)
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=contract_urn, aspect=status))

    print(f"Published Data Contract {contract_urn} for {entity_urn}")
    return contract_urn


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
        "Publishes dataset, task, job, lineage, validation, and contract metadata to DataHub (this script).",
        upstream_jobs=[gold_gate_job],
    )

    # ------------------------------------------------------------------
    # Data validation (assertions).
    #
    # Driven by the real results silver_quality_checks.py writes out --
    # see load_silver_quality_report(). Each entry in report["checks"]
    # becomes its own assertion in DataHub, so the Validation tab reflects
    # what actually happened in run_silver_quality_gate, not a placeholder.
    # ------------------------------------------------------------------
    report = load_silver_quality_report()
    check_results = report.get("checks", {})
    row_counts = report.get("row_counts", {})

    silver_key_columns = {
        "policyholders": (silver_policyholders, "customer_id"),
        "policies": (silver_policies, "policy_id"),
        "claims": (silver_claims, "claim_id"),
        "payments": (silver_payments, "payment_id"),
    }

    for table_name, (entity_urn, key_col) in silver_key_columns.items():
        row_count_check = f"{table_name}_row_count_positive"
        not_null_check = f"{table_name}_{key_col}_not_null"
        unique_check = f"{table_name}_{key_col}_unique"

        if row_count_check in check_results:
            emit_row_count_assertion(
                emitter, entity_urn, f"silver-{table_name}-row-count-positive",
                min_rows=0,
                observed_row_count=row_counts.get(table_name, 0),
            )
        if not_null_check in check_results:
            emit_custom_check_assertion(
                emitter, entity_urn, f"silver-{table_name}-{key_col}-not-null",
                category="Not Null",
                description=f"{key_col} must be non-null on every row of silver_delta.{table_name}.",
                passed=check_results[not_null_check],
                field=key_col,
            )
        if unique_check in check_results:
            emit_custom_check_assertion(
                emitter, entity_urn, f"silver-{table_name}-{key_col}-unique",
                category="Uniqueness",
                description=f"{key_col} must have no duplicate business keys after Silver dedup on {table_name}.",
                passed=check_results[unique_check],
                field=key_col,
            )

    if "claims_amount_non_negative" in check_results:
        emit_custom_check_assertion(
            emitter, silver_claims, "silver-claims-amount-non-negative",
            category="Business Rule",
            description="claim_amount must never be negative.",
            passed=check_results["claims_amount_non_negative"],
            field="claim_amount",
        )
    if "payments_amount_non_negative" in check_results:
        emit_custom_check_assertion(
            emitter, silver_payments, "silver-payments-amount-non-negative",
            category="Business Rule",
            description="amount must never be negative.",
            passed=check_results["payments_amount_non_negative"],
            field="amount",
        )

    # ------------------------------------------------------------------
    # Gold data validation (assertions).
    #
    # Driven by the real results quality_checks_clickhouse.py persisted to
    # gold_insurance.quality_check_results -- see load_gold_quality_results().
    # Every check in that script's CHECKS list becomes its own assertion in
    # DataHub, attached to the table it actually validates.
    # ------------------------------------------------------------------
    gold_results = load_gold_quality_results()

    # (dataset_urn, category, field) for each check_name in quality_checks_clickhouse.py.
    gold_check_metadata = {
        "dim_customer_unique_customer_id": (dim_customer, "Uniqueness", "customer_id"),
        "dim_policy_unique_policy_id": (dim_policy, "Uniqueness", "policy_id"),
        "fact_claim_unique_claim_id": (fact_claims, "Uniqueness", "claim_id"),
        "fact_payment_unique_payment_id": (fact_payment_attempts, "Uniqueness", "payment_id"),
        "fact_claim_fk_not_null": (fact_claims, "Referential Integrity", None),
        "fact_payment_fk_not_null": (fact_payment_attempts, "Referential Integrity", None),
        "claim_amount_non_negative": (fact_claims, "Business Rule", "claim_amount"),
        "payment_amount_non_negative": (fact_payment_attempts, "Business Rule", "amount"),
        "feature_payment_failure_rate_valid": (feat_customer_90d, "Business Rule", "f_customer_payment_failure_rate_90d"),
        "feature_unique_customer_as_of": (feat_customer_90d, "Uniqueness", "as_of_date"),
        "obt_claims_unique_claim_id": (obt_claims_enriched, "Uniqueness", "claim_id"),
        "obt_claims_amount_non_negative": (obt_claims_enriched, "Business Rule", "claim_amount"),
    }

    fact_claims_assertion_urns = []
    for check_name, (entity_urn, category, field) in gold_check_metadata.items():
        if check_name not in gold_results:
            continue
        status, failure_count = gold_results[check_name]
        assertion_urn = emit_custom_check_assertion(
            emitter, entity_urn, f"gold-{check_name.replace('_', '-')}",
            category=category,
            description=f"{check_name} ({failure_count} failing rows at last run).",
            passed=(status == "PASS"),
            field=field,
        )
        if entity_urn == fact_claims:
            fact_claims_assertion_urns.append(assertion_urn)

    # ------------------------------------------------------------------
    # Data contract.
    #
    # Bundles the real fact_claims assertions above into a single named,
    # public promise about gold_insurance.fact_claims that downstream
    # consumers can check. No freshness contract yet -- quality_checks_
    # clickhouse.py doesn't run a freshness check, so we don't fabricate one.
    # ------------------------------------------------------------------
    if fact_claims_assertion_urns:
        emit_data_contract(
            emitter,
            fact_claims,
            contract_id="fact-claims-contract",
            quality_assertion_urns=fact_claims_assertion_urns,
        )

    print("DataHub lineage, validation, and contract publishing completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"DataHub lineage publishing failed: {exc}", file=sys.stderr)
        sys.exit(1)