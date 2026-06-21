"""
So now Flink has already created the streaming features and written them into Kafka. 
This Python script takes those feature messages and saves them into ClickHouse.

Step 4 of the streaming path:
Kafka feature topic → Python consumer → ClickHouse table (gold_insurance.feat_stream_30m)

The Flink connector landscape in the vanilla image has no ClickHouse sink, so we
keep Flink doing the windowing and use a small, robust Python consumer to load
the feature topic into ClickHouse. This matches the documented architecture
(... -> features topic -> ClickHouse) and is easy to run/grade locally.

For coursework this drains the current backlog and stops once the topic is idle
(consumer_timeout_ms). In production this would run continuously as a service.


"""

from __future__ import annotations

import json
import os
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "insurance_events_features"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")

TARGET_TABLE = f"{CLICKHOUSE_DATABASE}.feat_stream_30m"
COLUMNS = [
    "customer_id",
    "window_start",
    "window_end",
    "f_stream_quote_views_30m",
    "f_stream_claim_submitted_30m",
    "f_stream_payment_failed_30m",
    "f_stream_burst_activity_flag",
]
BATCH_SIZE = 20_000 # This script waits until it reads 20k messages from Kafka, then writes them to ClickHouse in one batch. This is more efficient than writing one by one.
IDLE_TIMEOUT_MS = 10_000 # If no new messages arrive in Kafka for 10 seconds, we assume we're done and stop the consumer. In production, we might want this to run indefinitely instead of stopping on idle.


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def create_target_table(client) -> None:
    """Recreate the streaming feature table so each run produces a clean load."""
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    client.command(f"DROP TABLE IF EXISTS {TARGET_TABLE}") # For coursework, we drop and recreate the table on each run to ensure a clean slate. In production, we would likely want to keep existing data and append to it instead.
    client.command(
        f"""
        CREATE TABLE {TARGET_TABLE}
        (
            customer_id String,
            window_start DateTime,
            window_end DateTime,
            f_stream_quote_views_30m UInt64,
            f_stream_claim_submitted_30m UInt64,
            f_stream_payment_failed_30m UInt64,
            f_stream_burst_activity_flag Bool,
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree -- MergeTree is the most common ClickHouse engine for analytical queries over large datasets.
        ORDER BY (window_start, customer_id)
        """
    )


def parse_ts(value: str) -> datetime:
    """Parse Flink ISO-8601 timestamps; ClickHouse DateTime is second precision."""
    return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")

# Convert the incoming Kafka message (a dict) into a list of values in the same order as the COLUMNS list, so we can insert into ClickHouse.
def to_row(event: dict) -> list:
    return [
        str(event["customer_id"]),
        parse_ts(event["window_start"]),
        parse_ts(event["window_end"]),
        int(event["f_stream_quote_views_30m"]),
        int(event["f_stream_claim_submitted_30m"]),
        int(event["f_stream_payment_failed_30m"]),
        bool(event["f_stream_burst_activity_flag"]),
    ]


def main() -> None:
    client = clickhouse_client()
    create_target_table(client)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP, 
        group_id="clickhouse_stream_sink", # Kafka consumer group name. Kafka uses consumer groups to track which consumer is reading what. 
        auto_offset_reset="earliest", # If there's no committed offset for this consumer group (e.g. on the first run), start reading from the earliest messages in the topic.
        consumer_timeout_ms=IDLE_TIMEOUT_MS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")), # Kafka message value is JSON bytes, so we decode it to a dict.
    )

    batch: list[list] = []
    total = 0

    def flush() -> None:
        nonlocal batch, total
        if batch:
            client.insert(TARGET_TABLE, batch, column_names=COLUMNS)
            total += len(batch)
            print(f"inserted {len(batch)} rows (running total {total})")
            batch = []
    '''For every Kafka message: read message -> deserialize JSON -> convert to row format -> add to batch -> if batch size >= 20k, write batch to ClickHouse and flush batch'''
    for message in consumer:
        batch.append(to_row(message.value))
        if len(batch) >= BATCH_SIZE: # flush if batch reaches 20k rows
            flush()
    flush() # flush any remaining rows after exiting the loop (e.g. if we had 5k rows left after the last batch of 20k)
    consumer.close() # Close the Kafka consumer connection.

    table_count = client.query(f"SELECT count() FROM {TARGET_TABLE}").result_rows[0][0]
    print(f"done. consumed {total} feature messages; {TARGET_TABLE} now has {table_count} rows")


if __name__ == "__main__":
    main()
