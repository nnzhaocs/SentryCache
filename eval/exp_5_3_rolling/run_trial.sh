#!/usr/bin/env bash
# Run one §5.3 trial.  Usage:
#   run_trial.sh <condition> <trial_id>
#
#   condition: stock | sentrycache | nv
#   trial_id : free-form, used for output dir
#
# Pre-condition: timeline deployment + shared-cache deployed (manifests.yaml).
# Steps per trial:
#   1. FLUSHALL on shared-cache (cold)
#   2. Patch deployment to v1 / REDIS_DB=0; replicas=3; wait Ready
#   3. 30 s warmup (probe drives load to populate cache)
#   4. Trigger rolling update to v2 (with the per-condition env var set)
#   5. 60 s observe (probe runs through the rollout)
#   6. Save jsonl + summary

set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
COND=${1:?condition: stock|sentrycache|nv}
TRIAL=${2:?trial id}
OUT="${RESULT_ROOT}/exp_5_3/${COND}/trial_${TRIAL}"
mkdir -p "$OUT"

NS=sc-eval
TIMELINE_URL="http://timeline.sc-eval.svc.cluster.local"  # in-cluster
PROBE_URL="${PROBE_URL:-http://127.0.0.1:18080}" # local port-forward endpoint

echo "[$(date -u +%H:%M:%S)] === trial ${COND}/${TRIAL}: reset ==="
# 1. flush cache (use the cache pod itself; no redis-cli on the controller node)
CACHE_POD=$(kubectl get pod -n $NS -l app=shared-cache -o name | head -1)
kubectl exec -n $NS "$CACHE_POD" -- redis-cli -n 0 FLUSHDB >/dev/null
kubectl exec -n $NS "$CACHE_POD" -- redis-cli -n 1 FLUSHDB >/dev/null

# 2. reset to v1 baseline (REDIS_DB=0)
kubectl set env -n $NS deploy/timeline VERSION=v1 REDIS_DB=0 >/dev/null
kubectl rollout status -n $NS deploy/timeline --timeout=120s >/dev/null
# Force fresh pods so any stale state is gone
kubectl rollout restart -n $NS deploy/timeline >/dev/null
kubectl rollout status -n $NS deploy/timeline --timeout=120s >/dev/null

# Get the Service ClusterIP for probe target
TL_IP=$(kubectl get svc -n $NS timeline -o jsonpath='{.spec.clusterIP}')
TIMELINE_URL="http://${TL_IP}"

echo "[$(date -u +%H:%M:%S)] === warmup 30s (drives cache fill via v1 pods) ==="
python3 ${SENTRYCACHE_ROOT}/eval/exp_5_3_rolling/probe.py \
  --url "$TIMELINE_URL" --duration 30 --rps 60 \
  --out "$OUT" --label warmup &
WARMUP_PID=$!
wait $WARMUP_PID || true

echo "[$(date -u +%H:%M:%S)] === trigger rolling update to v2 (cond=$COND) ==="
case "$COND" in
  stock|nv)
    # v2 with REDIS_DB=0 (shared with v1) — stale reads expected
    kubectl set env -n $NS deploy/timeline VERSION=v2 REDIS_DB=0 >/dev/null
    ;;
  sentrycache)
    # v2 with REDIS_DB=1 (isolated) — fresh
    kubectl set env -n $NS deploy/timeline VERSION=v2 REDIS_DB=1 >/dev/null
    ;;
  *) echo "unknown condition $COND"; exit 2;;
esac

echo "[$(date -u +%H:%M:%S)] === observe 60s through rollout ==="
python3 ${SENTRYCACHE_ROOT}/eval/exp_5_3_rolling/probe.py \
  --url "$TIMELINE_URL" --duration 60 --rps 80 \
  --out "$OUT" --label observe

echo "[$(date -u +%H:%M:%S)] === trial ${COND}/${TRIAL} done ==="
ls -la "$OUT"
