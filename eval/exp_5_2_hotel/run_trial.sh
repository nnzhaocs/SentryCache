#!/usr/bin/env bash
# §5.2.1c HotelReservation prefetch trial. Usage:
#   run_trial.sh <condition> <trial_id>
#
#   condition: stock | sentrycache | np
#
# Pre-condition: sc-eval-hotel has the patched profile deployment (per-pod
# memcached sidecar). Controller for sentrycache/np already started with
# CACHE_BACKEND=memcached.
set -e
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
: "${DEATHSTARBENCH_ROOT:?set DEATHSTARBENCH_ROOT to the DeathStarBench checkout}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
COND=${1:?condition: stock|sentrycache|np}
TRIAL=${2:?trial id}
NS=sc-eval-hotel
REMOTE_WRK2="${REMOTE_WRK2:-/usr/local/bin/wrk}"
REMOTE_WORKLOAD_SCRIPT="${REMOTE_WORKLOAD_SCRIPT:?set REMOTE_WORKLOAD_SCRIPT}"
LOADGEN_SSH_HOST="${LOADGEN_SSH_HOST:?set LOADGEN_SSH_HOST}"
LOADGEN_SSH_USER="${LOADGEN_SSH_USER:-ubuntu}"
SSH_TARGET="${LOADGEN_SSH_USER}@${LOADGEN_SSH_HOST}"
FE_IP=$(kubectl get svc -n $NS frontend -o jsonpath='{.spec.clusterIP}')
TARGET_ENDPOINT="${TARGET_ENDPOINT:-http://${FE_IP}:5000}"
OUT=${RESULT_ROOT}/exp_5_2_hotel/${COND}/trial_${TRIAL}
mkdir -p "$OUT"

ts() { date -u +%H:%M:%S; }

echo "[$(ts)] === ${COND}/${TRIAL} reset ==="
kubectl scale -n $NS deploy/profile --replicas=3 >/dev/null
kubectl rollout restart -n $NS deploy/profile >/dev/null
kubectl rollout status -n $NS deploy/profile --timeout=180s >/dev/null

echo "[$(ts)] === warmup 60s @ 200 RPS ==="
ssh -n "$SSH_TARGET" "$REMOTE_WRK2 -t 8 -c 100 -d 60s -R 200 -s $REMOTE_WORKLOAD_SCRIPT $TARGET_ENDPOINT > /tmp/hr_${COND}_${TRIAL}_warm.log 2>&1"

echo "[$(ts)] === observe phase: 25s pre + scale 3->4 + 90s post ==="
MARK=$(python3 -c "import time; print(time.time()+25)")

ssh -n "$SSH_TARGET" "$REMOTE_WRK2 -t 8 -c 100 -d 115s -L -R 200 -s $REMOTE_WORKLOAD_SCRIPT $TARGET_ENDPOINT > /tmp/hr_${COND}_${TRIAL}_obs.log 2>&1" &
LOAD_PID=$!

sleep 25
echo "[$(ts)] === SCALE 3 -> 4 ==="
kubectl scale -n $NS deploy/profile --replicas=4 >/dev/null
NEW_POD=$(kubectl get pods -n $NS -l io.kompose.service=profile --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
echo "expected new pod: $NEW_POD"

wait $LOAD_PID
ssh -n "$SSH_TARGET" "cat /tmp/hr_${COND}_${TRIAL}_warm.log" > "$OUT/wrk2_warmup.log"
ssh -n "$SSH_TARGET" "cat /tmp/hr_${COND}_${TRIAL}_obs.log" > "$OUT/wrk2_observe.log"
echo "trigger=$MARK new_pod=$NEW_POD" > "$OUT/event_meta.txt"
echo "[$(ts)] === ${COND}/${TRIAL} done ==="
