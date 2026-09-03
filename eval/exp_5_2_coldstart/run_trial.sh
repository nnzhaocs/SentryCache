#!/usr/bin/env bash
# §5.2 trial runner.  Usage:
#   run_trial.sh <condition> <trial_id>
#
#   condition: stock | sentrycache | np
#   trial_id : free-form
#
# Pre-condition: cache-svc deployed, controller already started for sentrycache/np.
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
COND=${1:?condition: stock|sentrycache|np}
TRIAL=${2:?trial id}
NS=sc-eval
OUT="${RESULT_ROOT}/exp_5_2/${COND}/trial_${TRIAL}"
mkdir -p "$OUT"
LOADGEN=${SENTRYCACHE_ROOT}/eval/exp_5_4_migration/loadgen.py

ts() { date -u +%H:%M:%S; }

echo "[$(ts)] === ${COND}/${TRIAL} reset to 3 replicas ==="
kubectl scale -n $NS deploy/cache-svc --replicas=3 >/dev/null
kubectl rollout restart -n $NS deploy/cache-svc >/dev/null
kubectl rollout status -n $NS deploy/cache-svc --timeout=180s >/dev/null

echo "[$(ts)] === warmup 60s (3 replicas, hash-routed) ==="
python3 "$LOADGEN" \
  --namespace $NS --label app=cache-svc --port 8080 \
  --duration 60 --rps 80 \
  --zipf-alpha 0.9 --zipf-n 5000 \
  --out "$OUT" --label-out warmup

echo "[$(ts)] === observe: 25s pre + scale 3->4 + 90s post ==="
MARK=$(python3 -c "import time; print(time.time()+25)")

python3 "$LOADGEN" \
  --namespace $NS --label app=cache-svc --port 8080 \
  --duration 115 --rps 80 \
  --zipf-alpha 0.9 --zipf-n 5000 \
  --out "$OUT" --label-out observe --mark "$MARK" &
LOAD_PID=$!

sleep 25
echo "[$(ts)] === SCALE 3 -> 4 ==="
kubectl scale -n $NS deploy/cache-svc --replicas=4 >/dev/null
NEW_POD=$(kubectl get pods -n $NS -l app=cache-svc --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
echo "expected new pod: $NEW_POD"

wait $LOAD_PID
echo "trigger=$MARK new_pod=$NEW_POD" > "$OUT/event_meta.txt"
echo "[$(ts)] === ${COND}/${TRIAL} done ==="
ls "$OUT"
