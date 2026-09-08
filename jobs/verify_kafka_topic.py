"""
Verify that a Kafka topic actually received messages.

This is step 2 of the streaming path: after stream_json_to_kafka.py publishes the
generated events, we confirm the raw topic exists and is non-empty before we
spend time starting the Flink job. It sums the end offsets across all partitions
(the total number of messages produced) and prints one sample record so a human
can eyeball the schema. Exits non-zero if the topic is missing or empty, so it
can also act as a gate.
"""

import json
import sys

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

BOOTSTRAP = "kafka:29092"
TOPIC = "insurance_events_raw"


def main() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    partition_ids = consumer.partitions_for_topic(TOPIC)
    if not partition_ids:
        print(f"FAIL: topic '{TOPIC}' does not exist or has no partitions")
        consumer.close()
        sys.exit(1)

    partitions = [TopicPartition(TOPIC, p) for p in partition_ids]
    end_offsets = consumer.end_offsets(partitions)
    total_messages = sum(end_offsets.values())

    print(f"topic: {TOPIC}")
    print(f"partitions: {sorted(partition_ids)}")
    print(f"total messages (sum of end offsets): {total_messages}")

    # Read a single record so the schema is visible in the logs.
    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)
    sample = next(iter(consumer), None)
    if sample is not None:
        print("sample event:", json.dumps(sample.value, default=str))
    consumer.close()

    if total_messages <= 0:
        print(f"FAIL: topic '{TOPIC}' is empty")
        sys.exit(1)
    print("PASS: topic has messages")


if __name__ == "__main__":
    main()
