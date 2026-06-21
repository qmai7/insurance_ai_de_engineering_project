"""
Publish DataHub metadata and lineage for the streaming path.

Lineage:
  file: streaming.insurance_events (generated JSONL)
      -> kafka: insurance_events_raw         (produced by stream_json_to_kafka.py)
      -> flink: insurance_stream_features    (HOP-window aggregation job)
      -> kafka: insurance_events_features
      -> clickhouse: gold_insurance.feat_stream_30m  (stream_features_to_clickhouse.py)

Registering the Flink job as a DataFlow (orchestrator='flink') makes Flink show
up as a platform, and the DataJob connects the raw topic to the features topic so
you can trace which job produced the streaming features. Run after the streaming
pipeline has executed:

  docker exec insurance_airflow_scheduler python /opt/airflow/jobs/publish_streaming_lineage.py
"""

from __future__ import annotations

import os
import sys

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
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
FLINK_UI_URL = os.getenv("FLINK_UI_URL", "http://localhost:8081")
FLINK_JOB_NAME = "insurance_stream_features"


def dataset_urn(platform: str, name: str) -> str:
    return make_dataset_urn(platform=platform, name=name, env=ENV)


def emit_dataset_properties(emitter: DatahubRestEmitter, urn: str, name: str, description: str) -> None:
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(name=name, description=description),
        )
    )
    print(f"Published dataset properties: {urn}")


def emit_lineage(emitter: DatahubRestEmitter, downstream_urn: str, upstream_urns: list[str]) -> None:
    upstreams = [
        UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
        for u in upstream_urns
    ]
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=UpstreamLineageClass(upstreams=upstreams),
        )
    )
    print(f"Published lineage: {upstream_urns} -> {downstream_urn}")


def main() -> None:
    print(f"Connecting to DataHub GMS: {DATAHUB_GMS_URL}")
    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)

    # Datasets along the streaming path.
    src_jsonl = dataset_urn("file", "streaming.insurance_events")
    raw_topic = dataset_urn("kafka", "insurance_events_raw")
    features_topic = dataset_urn("kafka", "insurance_events_features")
    ch_features = dataset_urn("clickhouse", "gold_insurance.feat_stream_30m")

    datasets = {
        src_jsonl: ("Streaming source events", "Generated JSONL insurance events used as the streaming source."),
        raw_topic: ("Kafka insurance_events_raw", "Raw insurance events replayed into Kafka."),
        features_topic: ("Kafka insurance_events_features", "30-min HOP-window streaming features emitted by Flink."),
        ch_features: ("Gold feat_stream_30m", "ClickHouse table of sliding 30-min streaming features per customer."),
    }
    for urn, (name, description) in datasets.items():
        emit_dataset_properties(emitter, urn, name, description)

    # Flink job as an orchestrator so 'Flink' appears as a platform and the
    # transformation task links the raw topic to the features topic.
    flow_urn = make_data_flow_urn("flink", FLINK_JOB_NAME, cluster=ENV)
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=FLINK_JOB_NAME,
                description="Flink SQL streaming job: event-time HOP-window feature aggregation.",
                externalUrl=FLINK_UI_URL,
            ),
        )
    )
    print(f"Published data flow: {flow_urn}")

    job_urn = make_data_job_urn_with_flow(flow_urn, "hop_window_aggregation")
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name="hop_window_aggregation",
                type=AzkabanJobTypeClass.COMMAND,
                description="Aggregates raw events into 5-min sliding / 30-min streaming features.",
                flowUrn=flow_urn,
                externalUrl=FLINK_UI_URL,
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=[raw_topic],
                outputDatasets=[features_topic],
            ),
        )
    )
    print(f"Published data job: {job_urn}")

    # Dataset-to-dataset lineage along the whole streaming path.
    emit_lineage(emitter, raw_topic, [src_jsonl])
    emit_lineage(emitter, features_topic, [raw_topic])
    emit_lineage(emitter, ch_features, [features_topic])

    print("DataHub streaming lineage publishing completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"DataHub streaming lineage publishing failed: {exc}", file=sys.stderr)
        sys.exit(1)
