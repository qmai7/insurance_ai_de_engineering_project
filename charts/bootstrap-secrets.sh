#!/usr/bin/env bash
##
# Create the namespaces, ServiceAccounts and Secrets the charts expect.
#
# Run once per cluster, before deploying. The cluster is ephemeral, so this runs
# again after every `terraform destroy` — hence a script rather than a list of
# commands in a doc.
#
# Nothing here is committed: values are generated locally and live only in the
# cluster, which is what keeps database and session keys out of git (§14).
#
#   ./charts/bootstrap-secrets.sh [data-namespace] [ml-namespace]
##
set -euo pipefail

NS="${1:-data-ns}"
ML_NS="${2:-ml-ns}"

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

# ---------------------------------------------------------------------------
# ml-ns: MLflow tracking server and training jobs.
#
# The ServiceAccounts are created here rather than by the charts because the
# Workload Identity annotation needs the GCP service-account email, which comes
# from Terraform state. Baking one project's email into a committed chart would
# make the repo non-portable, and reading it here keeps the single source of
# truth in Terraform.
# ---------------------------------------------------------------------------
echo
kubectl create namespace "$ML_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

GSA=$(terraform -chdir="$(dirname "$0")/../terraform" output -raw data_platform_service_account 2>/dev/null || true)
if [[ -z "$GSA" ]]; then
  echo "WARNING: could not read data_platform_service_account from terraform output."
  echo "         ml-ns ServiceAccounts will be created without the Workload Identity"
  echo "         annotation, and MLflow will fail on its first artifact write."
fi

# Two identities, matching the two bindings in terraform.tfvars: the tracking
# server writes artifacts, a training run reads features and writes artifacts.
for KSA in mlflow training; do
  kubectl create serviceaccount "$KSA" -n "$ML_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if [[ -n "$GSA" ]]; then
    kubectl annotate serviceaccount "$KSA" -n "$ML_NS" \
      "iam.gke.io/gcp-service-account=$GSA" --overwrite >/dev/null
  fi
  echo "$ML_NS/$KSA: ServiceAccount ready${GSA:+ (impersonates $GSA)}"
done

# MLflow's backend store. Same Postgres instance as Airflow, separate database,
# reached across namespaces — hence the FQDN. The password is the one above,
# because it is the same server; a second password would need a second role.
MLFLOW_DB=mlflow
PGHOST="postgres.${NS}.svc.cluster.local"

kubectl create secret generic mlflow-db -n "$ML_NS" \
  --from-literal="username=${PGUSER}" \
  --from-literal="password=${PGPASS}" \
  --from-literal="database=${MLFLOW_DB}" \
  --from-literal="uri=postgresql://${PGUSER}:${PGPASS}@${PGHOST}:5432/${MLFLOW_DB}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "$ML_NS/mlflow-db: connection string written (database '${MLFLOW_DB}' on ${PGHOST})"
