#!/usr/bin/env bash
##
# Park the platform between work sessions without destroying it.
#
# GKE has no stop/start — a cluster bills for as long as it exists. But Autopilot
# charges for *pod resource requests*, so scaling every workload to zero removes
# the compute cost and lets Autopilot drain the nodes. What keeps billing is the
# cluster management fee (normally offset by GKE's free-tier credit) and the
# PersistentVolumes, which is a few cents a day for 20 GiB.
#
# The point of parking rather than destroying: the PVCs keep ClickHouse's Gold
# tables, Airflow's metadata and DAG history, and Redis's materialized features.
# Coming back is a scale-up instead of a half-hour rebuild.
#
#   ./charts/scale.sh down    # end of session
#   ./charts/scale.sh up      # next session
#   ./charts/scale.sh status
#
# Use `terraform destroy` instead when you are done for more than a few days —
# that stops the cluster fee and disk charges too, at the cost of rebuilding.
##
set -euo pipefail

ACTION="${1:-status}"

# Workload, namespace, replicas-when-up. StatefulSets are listed with the
# Deployments because scaling either to zero releases its pods while keeping the
# PVC bound.
WORKLOADS=(
  "deployment/airflow-scheduler data-ns 1"
  "deployment/airflow-webserver data-ns 1"
  "statefulset/postgres data-ns 1"
  "statefulset/clickhouse data-ns 1"
  "statefulset/redis api-serving-ns 1"
)

scale() {
  local target="$1" ns="$2" replicas="$3"
  if kubectl get "$target" -n "$ns" >/dev/null 2>&1; then
    kubectl scale "$target" -n "$ns" --replicas="$replicas" >/dev/null
    printf '  %-34s -> %s replicas\n' "$ns/$target" "$replicas"
  else
    printf '  %-34s (absent, skipped)\n' "$ns/$target"
  fi
}

case "$ACTION" in
  down)
    echo "Parking the platform (scaling all workloads to 0)..."
    for w in "${WORKLOADS[@]}"; do
      read -r target ns _ <<<"$w"
      scale "$target" "$ns" 0
    done
    echo
    echo "Nodes drain on their own once the pods are gone; give it a few minutes."
    echo "PVCs are retained, so no data is lost:"
    kubectl get pvc -A --no-headers 2>/dev/null | awk '{print "  "$1"/"$2"  "$4}'
    ;;

  up)
    echo "Resuming the platform..."
    # Postgres first: the Airflow webserver fails its startup probe and crashloops
    # if the metadata database is not reachable when it boots.
    for w in "statefulset/postgres data-ns 1" "statefulset/clickhouse data-ns 1" "statefulset/redis api-serving-ns 1"; do
      read -r target ns replicas <<<"$w"
      scale "$target" "$ns" "$replicas"
    done
    echo "  waiting for datastores to become ready..."
    kubectl wait --for=condition=ready pod -l app=postgres -n data-ns --timeout=300s >/dev/null 2>&1 || true
    kubectl wait --for=condition=ready pod -l app=clickhouse -n data-ns --timeout=300s >/dev/null 2>&1 || true
    for w in "deployment/airflow-scheduler data-ns 1" "deployment/airflow-webserver data-ns 1"; do
      read -r target ns replicas <<<"$w"
      scale "$target" "$ns" "$replicas"
    done
    echo
    echo "Autopilot must provision nodes again, so the first pods take a few minutes."
    echo "Then:  kubectl port-forward -n data-ns svc/airflow-webserver 8080:8080"
    ;;

  status)
    echo "Workloads:"
    kubectl get deploy,statefulset -A --no-headers 2>/dev/null |
      grep -vE "kube-system|gke-|gmp-" | awk '{print "  "$1"/"$2"  "$3}'
    echo "Nodes: $(kubectl get nodes --no-headers 2>/dev/null | wc -l)"
    echo "Pods (ours): $(kubectl get pods -A --no-headers 2>/dev/null | grep -cE "^(data-ns|api-serving-ns)" || true)"
    ;;

  *)
    echo "usage: $0 {down|up|status}" >&2
    exit 1
    ;;
esac
