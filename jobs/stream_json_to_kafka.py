"""
Publish generated streaming JSONL events into Kafka.

In real life, Kafka would usually receive events directly from applications,
web/mobile services, or CDC connectors. In this coursework project, we already
have generated JSONL events, so this script replays them into Kafka to simulate
an event stream.
"""

import json
from pathlib import Path

from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parents[1]
EVENT_FILE = BASE_DIR / "generated_insurance_data" / "streaming" / "insurance_events.jsonl"

# Create a Kafka producer that can send JSON-serialized messages to our local Kafka cluster.
producer = KafkaProducer(
    # Inside Docker Compose, services talk to Kafka through kafka:29092.
    bootstrap_servers="kafka:29092",
    # Kafka message value is JSON bytes.
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # Key by customer_id so events for the same customer are likely grouped together.
    key_serializer=lambda v: v.encode("utf-8") if v else None,
)

TOPIC = "insurance_events_raw"

sent = 0
with EVENT_FILE.open() as f:
    for line in f:
        event = json.loads(line)
        producer.send(TOPIC, key=event.get("customer_id"), value=event)
        sent += 1

producer.flush()
print(f"published {sent} events to {TOPIC}")
