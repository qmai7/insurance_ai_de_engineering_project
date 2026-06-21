"""
Gold quality gate for ClickHouse warehouse tables.

This runs after Gold modeling. It validates warehouse-level rules such as:
- business key uniqueness
- foreign-key availability
- non-negative measures
- valid feature ranges

The results are written to gold_insurance.quality_check_results so the project
has persistent evidence of quality checks, not just console logs.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")

CHECKS = [
    ("dim_customer_unique_customer_id", "select count() from (select customer_id from gold_insurance.dim_customer group by customer_id having count() > 1)"),
    ("dim_policy_unique_policy_id", "select count() from (select policy_id from gold_insurance.dim_policy group by policy_id having count() > 1)"),
    ("fact_claim_unique_claim_id", "select count() from (select claim_id from gold_insurance.fact_claims group by claim_id having count() > 1)"),
    ("fact_payment_unique_payment_id", "select count() from (select payment_id from gold_insurance.fact_payment_attempts group by payment_id having count() > 1)"),
    ("fact_claim_fk_not_null", "select count() from gold_insurance.fact_claims where customer_key is null or policy_key is null"),
    ("fact_payment_fk_not_null", "select count() from gold_insurance.fact_payment_attempts where customer_key is null or policy_key is null"),
    ("claim_amount_non_negative", "select count() from gold_insurance.fact_claims where claim_amount < 0"),
    ("payment_amount_non_negative", "select count() from gold_insurance.fact_payment_attempts where amount < 0"),
    ("feature_payment_failure_rate_valid", "select count() from gold_insurance.feat_customer_90d where f_customer_payment_failure_rate_90d < 0 or f_customer_payment_failure_rate_90d > 1"),
    ("obt_claims_unique_claim_id", "select count() from (select claim_id from gold_insurance.obt_claims_enriched group by claim_id having count() > 1)"),
    ("obt_claims_amount_non_negative", "select count() from gold_insurance.obt_claims_enriched where claim_amount < 0"),
]


def client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def main() -> None:
    ch = client()
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    ch.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.quality_check_results")
    ch.command(
        f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.quality_check_results
        (
            check_name String,
            status LowCardinality(String),
            failure_count UInt64,
            checked_at DateTime
        ) ENGINE = MergeTree
        ORDER BY (checked_at, check_name)
        """
    )

    rows = []
    for check_name, sql in CHECKS:
        failures = int(ch.query(sql).result_rows[0][0])
        rows.append((check_name, "PASS" if failures == 0 else "FAIL", failures, datetime.utcnow()))

    ch.insert(f"{CLICKHOUSE_DATABASE}.quality_check_results", rows, column_names=["check_name", "status", "failure_count", "checked_at"])

    print("check_name,status,failure_count")
    for check_name, status, failures, _ in rows:
        print(f"{check_name},{status},{failures}")

    if any(status == "FAIL" for _, status, _, _ in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()
