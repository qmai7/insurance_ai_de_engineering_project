#!/usr/bin/env bash
##
# Create the cluster Secrets the charts expect, with generated values.
#
# Run once per cluster, before deploying. The cluster is ephemeral, so this runs
# again after every `terraform destroy` — hence a script rather than a list of
# commands in a doc.
#
# Nothing here is committed: values are generated locally and live only in the
# cluster, which is what keeps database and session keys out of git (§14).
#
#   ./charts/bootstrap-secrets.sh [namespace]
##
set -euo pipefail

NS="${1:-data-ns}"

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Reuse an existing password rather than rotating it.
#
# This matters more than it looks: the official postgres image only applies
# POSTGRES_PASSWORD when it initialises an empty data directory. Generating a new
# password against an existing PVC would leave the database on the old one and
# Airflow unable to authenticate — with no error suggesting why.
if kubectl get secret postgres-credentials -n "$NS" >/dev/null 2>&1; then
  PGPASS=$(kubectl get secret postgres-credentials -n "$NS" -o jsonpath='{.data.password}' | base64 -d)
  echo "postgres-credentials: reusing existing password"
else
  PGPASS=$(openssl rand -hex 24)
  echo "postgres-credentials: generated new password"
fi

PGUSER=airflow
PGDB=airflow

kubectl create secret generic postgres-credentials -n "$NS" \
  --from-literal="username=${PGUSER}" \
  --from-literal="password=${PGPASS}" \
  --from-literal="database=${PGDB}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# The Airflow chart reads the whole SQLAlchemy URI from one key named
# `connection`, which is why this is a separate Secret rather than a reference to
# the one above.
kubectl create secret generic airflow-metadata-db -n "$NS" \
  --from-literal="connection=postgresql://${PGUSER}:${PGPASS}@postgres:5432/${PGDB}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "airflow-metadata-db: connection string written"

# Signs webserver session cookies. Kept stable across upgrades so the chart does
# not generate a fresh one and log every user out on each `helm upgrade`.
if kubectl get secret airflow-webserver-secret -n "$NS" >/dev/null 2>&1; then
  echo "airflow-webserver-secret: already present, left alone"
else
  kubectl create secret generic airflow-webserver-secret -n "$NS" \
    --from-literal="webserver-secret-key=$(openssl rand -hex 32)" >/dev/null
  echo "airflow-webserver-secret: generated"
fi

echo
echo "Secrets ready in namespace '$NS':"
kubectl get secrets -n "$NS" \
  postgres-credentials airflow-metadata-db airflow-webserver-secret \
  -o custom-columns=NAME:.metadata.name,KEYS:.data --no-headers 2>/dev/null |
  sed 's/map\[/ /; s/\]//' | awk '{print "  " $1}'
