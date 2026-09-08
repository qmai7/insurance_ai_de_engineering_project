#!/usr/bin/env bash
#
# Submit the streaming feature job to the Flink cluster via the SQL client.
#
# The vanilla Flink image has no Kafka connector, so we attach the downloaded
# flink-sql-connector-kafka JAR with -j (it is also shipped to the TaskManagers
# with the job). The SQL file is executed with -f; the INSERT INTO is a streaming
# query, so the client submits it to the cluster and returns while it keeps
# running. Intended to be run from inside the Flink JobManager container, e.g.:
#
#   docker exec insurance_flink_jobmanager bash /opt/flink/usrlib/run_flink_stream_job.sh
#
set -euo pipefail

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
USRLIB="${FLINK_HOME}/usrlib"
KAFKA_CONNECTOR_JAR="${USRLIB}/flink-sql-connector-kafka-3.2.0-1.18.jar"
SQL_FILE="${USRLIB}/insurance_stream_features.sql"

echo "Submitting Flink SQL job from ${SQL_FILE}"
echo "Using Kafka connector: ${KAFKA_CONNECTOR_JAR}"

"${FLINK_HOME}/bin/sql-client.sh" \
  -j "${KAFKA_CONNECTOR_JAR}" \
  -f "${SQL_FILE}"

echo "Submission finished. Check the Flink UI (http://localhost:8081) for a RUNNING job."
