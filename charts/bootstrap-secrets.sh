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

# Three identities, matching the ml-ns bindings in terraform.tfvars: the
# tracking server writes artifacts, a training run reads features and writes
# artifacts, and Kubeflow's `pipeline-runner` is what step pods actually run as.
#
# `pipeline-runner` is also created by `kubectl apply -k charts/kubeflow`, so the
# two overlap by design and the order does not matter. Creating it here first is
# safe: kubectl's three-way merge leaves annotations it does not manage alone, so
# a later KFP apply will not strip the Workload Identity annotation.
for KSA in mlflow training pipeline-runner; do
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

# ---------------------------------------------------------------------------
# Kubeflow Pipelines' own credentials.
#
# KFP ships these as committed manifests: MySQL as root with *no* password at
# all, and `minio`/`minio123` for its object store. charts/kubeflow deletes both
# so they are generated here instead — §14, and the same rule the rest of this
# script follows.
#
# Must run *before* `kubectl apply -k charts/kubeflow`: the api-server and the
# database pod both mount these at startup.
#
# Reused rather than rotated, for the reason documented at the top of this file:
# the mysql image only applies MYSQL_ROOT_PASSWORD to an empty data directory, so
# a fresh password against KFP's existing PVC would lock the api server out of
# its own database. Rotating means deleting mysql-pv-claim too.
# ---------------------------------------------------------------------------
echo

kfp_password() {
  local secret="$1" key="$2"
  if kubectl get secret "$secret" -n "$ML_NS" >/dev/null 2>&1; then
    kubectl get secret "$secret" -n "$ML_NS" -o jsonpath="{.data.${key}}" | base64 -d
  else
    openssl rand -hex 24
  fi
}

# KFP's metadata database. Upstream runs it as root with an empty password and
# MYSQL_ALLOW_EMPTY_PASSWORD, which charts/kubeflow replaces with a real one.
# Only applied to an empty data directory, hence the reuse — see above.
KFP_DBPASS=$(kfp_password mysql-secret password)

kubectl create secret generic mysql-secret -n "$ML_NS" \
  --from-literal="username=root" \
  --from-literal="password=${KFP_DBPASS}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "$ML_NS/mysql-secret: KFP database password set (user 'root')"

# SeaweedFS is KFP's internal S3-compatible artifact store. It reads this secret
# on every start to configure its own S3 user, so the value is authoritative
# rather than something it remembers — but the config lives on its PVC, so
# changing the key on an existing install needs seaweedfs-pvc deleted too.
SEAWEED_KEY=$(kfp_password mlpipeline-minio-artifact accesskey)
SEAWEED_SECRET=$(kfp_password mlpipeline-minio-artifact secretkey)

kubectl create secret generic mlpipeline-minio-artifact -n "$ML_NS" \
  --from-literal="accesskey=${SEAWEED_KEY}" \
  --from-literal="secretkey=${SEAWEED_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "$ML_NS/mlpipeline-minio-artifact: SeaweedFS credentials set"
