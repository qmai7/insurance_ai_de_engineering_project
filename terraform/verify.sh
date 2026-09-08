#!/usr/bin/env bash
##
# Step 1 acceptance test.
#
# Checks the three things Step 1 claims to deliver: the cluster is reachable, the
# lakehouse bucket is writable, and a pod can reach that bucket through Workload
# Identity with no service-account key mounted.
#
# The third check is the one that matters — a cluster and a bucket that cannot
# talk to each other would pass a naive "does it exist" test and then fail on the
# first Spark job of Step 2.
#
#   ./verify.sh
##
set -euo pipefail

cd "$(dirname "$0")"

PASS=0
FAIL=0

ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); }
head_() { printf '\n\033[1m%s\033[0m\n' "$1"; }

PROJECT=$(terraform output -raw project_id)
CLUSTER=$(terraform output -raw cluster_name)
REGION=$(terraform output -raw cluster_location)
BUCKET=$(terraform output -raw lakehouse_bucket)
GSA=$(terraform output -raw data_platform_service_account)

head_ "1. Cluster reachable"

STATUS=$(gcloud container clusters describe "$CLUSTER" \
  --region "$REGION" --project "$PROJECT" --format='value(status)' 2>/dev/null || echo MISSING)
[[ "$STATUS" == "RUNNING" ]] && ok "cluster $CLUSTER is RUNNING" || bad "cluster status is $STATUS"

gcloud container clusters get-credentials "$CLUSTER" \
  --region "$REGION" --project "$PROJECT" >/dev/null 2>&1

if kubectl version --request-timeout=20s >/dev/null 2>&1; then
  ok "kubectl authenticates to the API server"
else
  bad "kubectl cannot reach the API server"
fi

if kubectl get ns kube-system >/dev/null 2>&1; then
  ok "control plane serving requests ($(kubectl get ns --no-headers | wc -l) namespaces)"
else
  bad "control plane not responding"
fi

# Autopilot reports no nodes until the first workload forces provisioning, so an
# empty node list here is expected rather than a failure.
head_ "2. Autopilot confirmed"
if gcloud container clusters describe "$CLUSTER" --region "$REGION" --project "$PROJECT" \
     --format='value(autopilot.enabled)' 2>/dev/null | grep -qi true; then
  ok "Autopilot mode enabled"
else
  bad "cluster is not Autopilot"
fi

head_ "3. Lakehouse bucket read/write"
if gcloud storage ls "gs://$BUCKET" >/dev/null 2>&1; then
  ok "bucket gs://$BUCKET exists"
else
  bad "bucket gs://$BUCKET not reachable"
fi

PROBE="gs://$BUCKET/_verify/probe-$$.txt"
if echo "step1-probe" | gcloud storage cp - "$PROBE" >/dev/null 2>&1 \
   && [[ "$(gcloud storage cat "$PROBE" 2>/dev/null)" == "step1-probe" ]]; then
  ok "object write + read back succeeded"
  gcloud storage rm "$PROBE" >/dev/null 2>&1 || true
else
  bad "could not write/read an object"
fi

head_ "4. Workload Identity end-to-end"

# These two are what Step 2's Airflow and Spark will actually use; the pod below
# is throwaway and removed at the end.
kubectl create namespace data-ns --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl create serviceaccount airflow -n data-ns --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl annotate serviceaccount airflow -n data-ns \
  "iam.gke.io/gcp-service-account=$GSA" --overwrite >/dev/null
ok "namespace data-ns + KSA airflow annotated for $GSA"

POD=wi-probe-$$
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: data-ns
spec:
  serviceAccountName: airflow
  restartPolicy: Never
  containers:
    - name: probe
      image: google/cloud-sdk:alpine
      command: ["sh", "-c"]
      args:
        - |
          set -e
          echo "identity: \$(gcloud config get-value account 2>/dev/null)"
          echo "wi-ok" | gcloud storage cp - gs://$BUCKET/_verify/wi-probe.txt
          gcloud storage cat gs://$BUCKET/_verify/wi-probe.txt
          gcloud storage rm gs://$BUCKET/_verify/wi-probe.txt
      resources:
        requests:
          cpu: 250m
          memory: 512Mi
EOF

printf '  … waiting for pod (Autopilot provisions a node on first workload, ~2 min)\n'
trap 'kubectl delete pod "$POD" -n data-ns --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT

# Polled rather than `kubectl wait`, so a Failed pod is reported immediately
# instead of sitting out the full timeout.
PHASE=""
for _ in $(seq 1 100); do
  PHASE=$(kubectl get "pod/$POD" -n data-ns -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "$PHASE" == "Succeeded" || "$PHASE" == "Failed" ]] && break
  sleep 3
done

if [[ "$PHASE" == "Succeeded" ]]; then
  LOGS=$(kubectl logs "$POD" -n data-ns 2>/dev/null)
  if grep -q "wi-ok" <<<"$LOGS"; then
    ok "pod wrote to GCS via Workload Identity, no key mounted"
    printf '      as: %s\n' "$(grep identity: <<<"$LOGS" | cut -d' ' -f2-)"
  else
    bad "pod ran but GCS access failed"
    printf '%s\n' "$LOGS" | sed 's/^/      /'
  fi
else
  bad "pod did not succeed (phase=${PHASE:-unknown})"
  kubectl describe "pod/$POD" -n data-ns 2>/dev/null | tail -15 | sed 's/^/      /'
  kubectl logs "$POD" -n data-ns 2>/dev/null | tail -15 | sed 's/^/      /' || true
fi

head_ "Result"
printf '  %d passed, %d failed\n\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
