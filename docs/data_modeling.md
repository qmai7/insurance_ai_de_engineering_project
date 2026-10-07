# Data modeling

![Data modeling](/assets/data_modeling/Data_Modeling.png)

Built by `jobs/gold_clickhouse.py` from trusted Silver Delta tables.

| Dimension | Grain | Keys & columns |
|---|---|---|
| `dim_customer` | one per customer | `customer_key` (Surrogate Key), `customer_id` (Business Key), signup_ts, age, province, city, risk_segment, marketing_opt_in, `row_hash`, `valid_from_ts`, `valid_to_ts`, `is_current` |
| `dim_policy` | one per policy | `policy_key` (SK), `policy_id` (BK), `customer_key` (FK), policy_type, policy_start_date, policy_end_date, premium_amount, policy_status |
| `dim_date` | one per calendar date | `date_key` (yyyymmdd), calendar_date, year, month, day, day_of_week, is_weekend |

**Legend**: 
    - BK: Business Key 
    - SK: Surrogate Key - machine generated key, it is typically a sequential auto-incrementing integer or a globally unique identifier (GUID) used to join tables 
    - FK: Foreign Key - links to the primary key of another table

**Final Tables**

![Data modeling](/assets/data_modeling/final_tables.png)

**SCD2** 

![Data modeling](/assets/data_modeling/SCD2.png)

- `dim_date` is built from the union of all business dates (policy start/end, claim
  date, payment date), de-duplicated.
- **SCD strategy — `dim_customer` is SCD Type 2.** Each run compares the incoming
  Silver snapshot against the versions already stored in ClickHouse:
  - A `row_hash` over the tracked attributes (age, province, city, risk_segment,
    marketing_opt_in) detects changes.
  - **Unchanged** customer → the current version stays live.
  - **Changed** customer → the current version is closed (`is_current = false`,
    `valid_to_ts` set to the run timestamp) and a **new version** is opened
    (`is_current = true`, `valid_from_ts` = run timestamp, `valid_to_ts` NULL) with
    a fresh `customer_key`.
  - **New** customer → a first version is opened; **disappeared** customer →
    current version is soft-closed.
  - The surrogate key `customer_key` is unique **per version**; downstream
    `dim_policy` / feature joins resolve each customer to its **current** version's
    key. History is read back before the table is rewritten, so no version is lost.
  - Because static coursework data does not change between runs, version history
    appears when the source is re-generated with changed customer attributes and
    the pipeline is re-run. `dim_policy` remains Type-1.


## Feature store design

Two feature tables are implemented in `gold_insurance` (a future `feat_customer_unified`
join of the two is documented as next work, not built).

**Offline — `feat_customer_90d`** (`jobs/gold_clickhouse.py`)
- Grain: one row per (`customer_id`, `as_of_date`) — a time series, not a snapshot.
- Features: `f_customer_avg_claim_amount_90d`, `f_customer_total_claims_90d`,
  `f_customer_total_claim_amount_90d`, `f_customer_total_payments_90d`,
  `f_customer_payment_failure_rate_90d`.
- `as_of_date` values come from two places: every date a customer filed a claim
  (the training spine), plus the newest claim date in the dataset for *every*
  customer (the row that materializes into Redis for serving).
- **Point-in-time correctness:** only claims/payments in
  `[as_of_date - 90d, as_of_date)` are aggregated — no data later than the
  reference timestamp leaks in, and the strict upper bound also means the history
  behind a claim never includes that claim.
- The grain matters more than it looks. It was originally a single snapshot for
  all customers, which reads as correct and is unusable: Feast's point-in-time
  join takes the newest feature row at or before the entity timestamp, so a
  training pull keyed on `claim_date` matched only claims filed on the snapshot
  date and returned nulls for every older one.

**Streaming — `feat_stream_30m`** (Flink + `jobs/stream_features_to_clickhouse.py`)
- Grain: one per `customer_id` per sliding window (`window_start`, `window_end`).
- Features: `f_stream_quote_views_30m`, `f_stream_claim_submitted_30m`,
  `f_stream_payment_failed_30m`, `f_stream_burst_activity_flag`.
- 30-minute window, 5-minute slide (HOP), event-time with a 5-minute watermark for
  late events; `window_end` is the point-in-time reference for joins.
- Latest run: **196,485** feature rows across **9,706** customers.

## OBT design

| OBT | Grain | Purpose | Core columns |
|---|---|---|---|
| `obt_claims_enriched` | one per claim (transaction grain) | denormalized table for claim/loss BI & dashboards — no joins needed | claim_id, claim_date, claim_type, claim_status, claim_amount, policy_id, policy_type, policy_status, premium_amount, policy_start/end_date, claim_to_premium_ratio, days_since_policy_start, customer_id, province, city, risk_segment, age, marketing_opt_in, claim_year, claim_month, claim_day_of_week, claim_is_weekend |

Transaction-grain so BI questions (loss by policy_type/geography/time, loss ratio,
claim-status mix) resolve from one wide table. Joins claim → policy → customer →
date; `risk_segment` stays `Nullable` (schema evolution). Partitioned by
`toYYYYMM(claim_date)`, ordered by `(claim_date, policy_type, province, claim_id)`.