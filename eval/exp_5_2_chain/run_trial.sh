#!/usr/bin/env bash
# §5.2.2 ChainApp trial. Usage:
#   run_trial.sh <depth> <condition> <trial_id>
#
#   depth:     1..4
#   condition: stock | sentrycache
#
# Pre-condition: namespace sc-eval-chain exists with the right depth's
# layers deployed; controller (for sentrycache) targets chain-layer-1.
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
DEPTH=${1:?depth: 1..4}
COND=${2:?condition: stock|sentrycache}
TRIAL=${3:?trial id}
NS=sc-eval-chain
TARGET="chain-layer-1"
LOADGEN=${SENTRYCACHE_ROOT}/eval/exp_5_4_migration/loadgen.py
OUT=${RESULT_ROOT}/exp_5_2_chain/depth_${DEPTH}/${COND}/trial_${TRIAL}
mkdir -p "$OUT"

ts() { date -u +%H:%M:%S; }

echo "[$(ts)] === d=${DEPTH} ${COND}/${TRIAL} reset ==="
kubectl scale -n $NS deploy/${TARGET} --replicas=3 >/dev/null
kubectl rollout restart -n $NS deploy/${TARGET} >/dev/null
kubectl rollout status -n $NS deploy/${TARGET} --timeout=180s >/dev/null

echo "[$(ts)] === warmup 30s ==="
python3 "$LOADGEN" \
  --namespace $NS --label app=${TARGET} --port 8080 \
  --duration 30 --rps 80 \
  --zipf-alpha 1.3 --zipf-n 5000 \
  --out "$OUT" --label-out warmup

echo "[$(ts)] === observe: 15s pre + scale 3->4 + 60s post ==="
MARK=$(python3 -c "import time; print(time.time()+15)")

python3 "$LOADGEN" \
  --namespace $NS --label app=${TARGET} --port 8080 \
  --duration 75 --rps 80 \
  --zipf-alpha 1.3 --zipf-n 5000 \
  --out "$OUT" --label-out observe --mark "$MARK" &
LOAD_PID=$!

sleep 15
echo "[$(ts)] === SCALE 3 -> 4 ==="
kubectl scale -n $NS deploy/${TARGET} --replicas=4 >/dev/null

wait $LOAD_PID
echo "trigger=$MARK depth=$DEPTH" > "$OUT/event_meta.txt"
echo "[$(ts)] === d=${DEPTH} ${COND}/${TRIAL} done ==="
