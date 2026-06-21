-- Flink SQL streaming feature engineering job.
-- This helps answer a moving business question: "What happened with each customer in the last 30 minutes?"
--
-- Flow:
--   Kafka topic insurance_events_raw
--       -> Flink event-time HOP window aggregation
--       -> Kafka topic insurance_events_features
--   (stream_features_to_clickhouse.py then sinks the features topic into ClickHouse)
--
-- Run with flink/run_flink_stream_job.sh, which submits this file through the
-- Flink SQL client with the Kafka connector JAR attached.

-- Streaming job settings. parallelism.default=1 keeps the single-partition
-- coursework topic simple; the job runs detached on the cluster.
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'insurance_stream_features';
SET 'parallelism.default' = '1';

-- This does not create a table in database; it just tells Flink how to connect to the Kafka topic and interpret the data.
CREATE TABLE insurance_events_raw (
  event_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP(3),
  created_ts TIMESTAMP(3),
  customer_id STRING,
  policy_id STRING,
  province STRING,
  city STRING,
  channel STRING,
  device_type STRING,

  -- Watermark allows Flink to handle late events.
  -- Here we allow events to arrive up to 5 minutes late before closing windows.
  WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' MINUTE
) WITH (
  -- Kafka source connector to read the raw insurance events from the "insurance_events_raw" topic. This topic is produced by stream_json_to_kafka.py.
  'connector' = 'kafka',
  'topic' = 'insurance_events_raw',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink_insurance_features',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'scan.startup.mode' = 'earliest-offset'
);

-- This defines the output of our streaming feature engineering job, which will be written to a different Kafka topic, which is "insurance_events_features"
-- Each output row is one customer's features values for a 30-minute window
-- Example: 
-- customer_id: "cust123"
-- window_start: "2024-01-01T12:00:00.000Z"
-- window_end: "2024-01-01T12:30:00.000Z"
-- f_stream_quote_views_30m: 5
-- f_stream_claim_submitted_30m: 1
-- f_stream_payment_failed_30m: 0
-- f_stream_burst_activity_flag: true (if total events in window >= 10)
CREATE TABLE insurance_stream_features (
  customer_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  f_stream_quote_views_30m BIGINT,
  f_stream_claim_submitted_30m BIGINT,
  f_stream_payment_failed_30m BIGINT,
  f_stream_burst_activity_flag BOOLEAN
) WITH (
  -- Kafka sink connector to write the output features to a new Kafka topic. This topic will be consumed by stream_features_to_clickhouse.py to load into ClickHouse.
  'connector' = 'kafka',
  'topic' = 'insurance_events_features',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);


-- - every 5 minutes, Flink emits a new result (data gets updated every 5 minutes)
-- - each result looks back over the last 30 minutes
INSERT INTO insurance_stream_features
SELECT
  customer_id,
  window_start,
  window_end,
  SUM(CASE WHEN event_type = 'quote_view' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type = 'claim_submitted' THEN 1 ELSE 0 END),
  SUM(CASE WHEN event_type = 'payment_failed' THEN 1 ELSE 0 END),
  COUNT(*) >= 10
FROM TABLE(
  -- HOP creates rolling/sliding windows of 30 minutes that hop every 5 minutes, based on the event_timestamp field of the input data.
  -- So, Flink will create windows like:
  -- [12:00 - 12:30), [12:05 - 12:35), [12:10 - 12:40), etc.
  HOP(TABLE insurance_events_raw, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE, INTERVAL '30' MINUTE)
)
GROUP BY customer_id, window_start, window_end;
