"""
Publish generated streaming JSONL events into Kafka.

In real life, Kafka would usually receive events directly from applications,
web/mobile services, or CDC connectors. In this coursework project, we already
have generated JSONL events, so this script replays them into Kafka to simulate
an event stream.
"""

import json
import os
from pathlib import Path

import fsspec
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parents[1]

# On GKE the generated JSONL lives in Bronze on GCS — a pod has no copy of the
# repo's generated_insurance_data/. Without LAKEHOUSE_ROOT it falls back to the
# local file, as under Docker Compose. fsspec opens either (gs:// via gcsfs,
# authenticated through Workload Identity).
LAKEHOUSE_ROOT = (os.getenv("LAKEHOUSE_ROOT") or "").rstrip("/")
EVENT_FILE = (
    f"{LAKEHOUSE_ROOT}/bronze/streaming/insurance_events.jsonl"
    if LAKEHOUSE_ROOT
    else str(BASE_DIR / "generated_insurance_data" / "streaming" / "insurance_events.jsonl")
)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Create a Kafka producer that can send JSON-serialized messages to our Kafka cluster.
producer = KafkaProducer(
    # kafka:29092 is the in-network address under both Docker Compose and GKE.
    bootstrap_servers=BOOTSTRAP,
    # Kafka message value is JSON bytes.
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # Key by customer_id so events for the same customer are likely grouped together.
    key_serializer=lambda v: v.encode("utf-8") if v else None,
)

TOPIC = "insurance_events_raw"

sent = 0
print(f"reading {EVENT_FILE}")
with fsspec.open(EVENT_FILE, "rt") as f:
    for line in f:
        event = json.loads(line)
        producer.send(TOPIC, key=event.get("customer_id"), value=event)
        sent += 1

producer.flush()
print(f"published {sent} events to {TOPIC}")
