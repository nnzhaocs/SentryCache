#!/usr/bin/env bash
# §5.4 trial runner.  Usage:
#   run_trial.sh <condition> <scenario> <trial_id>
#
#   condition: stock | sentrycache | nm
#   scenario : scalein | crash
#   trial_id : free-form
#
# Pre-condition: cache-svc deployed, controller already started for sentrycache/nm.
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
COND=${1:?condition: stock|sentrycache|nm}
SCEN=${2:?scenario: scalein|crash}
TRIAL=${3:?trial id}
NS=sc-eval
OUT="${RESULT_ROOT}/exp_5_4/${COND}/${SCEN}/trial_${TRIAL}"
mkdir -p "$OUT"
LOADGEN=${SENTRYCACHE_ROOT}/eval/exp_5_4_migration/loadgen.py

ts() { date -u +%H:%M:%S; }

echo "[$(ts)] === ${COND}/${SCEN}/${TRIAL} reset ==="
# Bring cache-svc back to 3 replicas; restart so per-pod sidecars are empty.
kubectl scale -n $NS deploy/cache-svc --replicas=3 >/dev/null
kubectl rollout restart -n $NS deploy/cache-svc >/dev/null
kubectl rollout status -n $NS deploy/cache-svc --timeout=180s >/dev/null

TL_IP=$(kubectl get svc -n $NS cache-svc -o jsonpath='{.spec.clusterIP}')
TARGET_URL="http://${TL_IP}"

echo "[$(ts)] === warmup 60s (hash-routed) ==="
python3 "$LOADGEN" \
  --namespace $NS --label app=cache-svc --port 8080 \
  --duration 60 --rps 80 \
  --zipf-alpha 0.9 --zipf-n 5000 \
  --out "$OUT" --label-out warmup

echo "[$(ts)] === observe: 25s pre + trigger + 90s post ==="
MARK=$(python3 -c "import time; print(time.time()+25)")

python3 "$LOADGEN" \
  --namespace $NS --label app=cache-svc --port 8080 \
  --duration 115 --rps 80 \
  --zipf-alpha 0.9 --zipf-n 5000 \
  --out "$OUT" --label-out observe --mark "$MARK" &
LOAD_PID=$!

# Sleep until the mark, then trigger.
sleep 25
echo "[$(ts)] === trigger ${SCEN} ==="
case "$SCEN" in
  scalein)
    # Pick a victim pod, scale-down by 1.  K8s will preStop it (25s sleep) so
    # the controller has a window to migrate.
    VICTIM=$(kubectl get pods -n $NS -l app=cache-svc --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
    echo "victim: $VICTIM"
    kubectl scale -n $NS deploy/cache-svc --replicas=2 >/dev/null
    ;;
  crash)
    VICTIM=$(kubectl get pods -n $NS -l app=cache-svc --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
    echo "victim: $VICTIM"
    kubectl delete pod -n $NS "$VICTIM" --force --grace-period=0 >/dev/null
    ;;
esac

wait $LOAD_PID
echo "[$(ts)] === ${COND}/${SCEN}/${TRIAL} done ==="
echo "trigger=$MARK victim=$VICTIM" > "$OUT/event_meta.txt"
ls "$OUT"
