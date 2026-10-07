# ClickHouse storage-layer optimization 

## 1. Use `LowCardinality(String)` for low-cardinality columns. 

My batch DDLs use it everywhere it makes sense such as `province`,`claim_type`,`policy_type`,`payment_status`. Normally, a `String` column in ClickHouse stores the literal bytes of each value, for every single row. If we have 10 million claim rows and a `claim_type` column with values like "auto_collision", "water_damage", "theft" — even though there are only, say, 8 distinct claim types, ClickHouse stores those full strings 10 million times, once per row.

`LowCardinality(String)` changes the physical storage strategy: ClickHouse builds a dictionary — a small table mapping each distinct string to a compact integer ID `(e.g. "auto_collision" → 0, "water_damage" → 1, "theft" → 2)` — and then each row just stores the tiny integer instead of the full string. 

Benefits: 
- Storage: an integer (often 1-2 bytes) instead of a 10-20+ byte string, repeated millions of times, is a big compression win.
- Speed: filtering on integers are faster than string comparisons, so queries like `WHERE province = 'Quebec'` or `GROUP BY province` run quicker.


## 2. Partition by `date_key` 

```sql
PARTITION BY intDiv(ifNull(payment_date_key, 0), 100)

```

- `payment_date_key` is an integer like `20251104` (year-month-day, from the `dim_date` table's).
- `intDiv(20251104, 100) = 202511`. So this expression groups all dates within the same year+month into one bucket. `20251101` through `20251130` would all map to `202511`; December would map to `202512`.

This line tells ClickHouse: physically store rows on disk grouped into separate chunks ("partitions"), one chunk per month, based on each row's payment date.
So if run a query like this 

```sql
SELECT * FROM fact_payment_attempts
WHERE payment_date_key >= 20251101 AND payment_date_key < 20251201
```

ClickHouse can look at the query's date filter, figure out which partition(s) that range falls into, and skip reading the other partitions' files entirely. If you have 3 years of payment data split into 36 monthly partitions, and your query only needs one month, ClickHouse does roughly 1/36th the disk I/O instead of scanning everything and then filtering