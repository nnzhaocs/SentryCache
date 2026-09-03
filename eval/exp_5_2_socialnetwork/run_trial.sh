#!/usr/bin/env bash
# §5.2.1 SocialNetwork prefetch trial.
#
#   run_trial.sh <condition> <trial_id>
#
#   condition: stock | sentrycache | np
#
# Pre-condition: Service home-timeline-service selector already pointed at the
# standalone variant (io.kompose.service=home-timeline-service); cluster
# variant scaled to 0; standalone deployment at 3 replicas; controller for
# sentrycache/np already running with SC_NAMESPACE=sc-demo.
set -e
: "${RESULT_ROOT:?set RESULT_ROOT to a writable experiment output directory}"
: "${DEATHSTARBENCH_ROOT:?set DEATHSTARBENCH_ROOT to the DeathStarBench checkout}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
COND=${1:?condition: stock|sentrycache|np}
TRIAL=${2:?trial id}
NS=sc-demo
LOADGEN_SSH_HOST="${LOADGEN_SSH_HOST:?set LOADGEN_SSH_HOST}"
LOADGEN_SSH_USER="${LOADGEN_SSH_USER:-ubuntu}"
SSH_TARGET="${LOADGEN_SSH_USER}@${LOADGEN_SSH_HOST}"
TARGET_ENDPOINT="${TARGET_ENDPOINT:?set TARGET_ENDPOINT}"
REMOTE_WRK2="${REMOTE_WRK2:-/usr/local/bin/wrk}"
REMOTE_WORKLOAD_SCRIPT="${REMOTE_WORKLOAD_SCRIPT:?set REMOTE_WORKLOAD_SCRIPT}"
OUT=${RESULT_ROOT}/exp_5_2_sn/${COND}/trial_${TRIAL}
mkdir -p "$OUT"

ts() { date -u +%H:%M:%S; }

echo "[$(ts)] === ${COND}/${TRIAL} reset ==="
# Reset to 3 replicas and bounce them so caches are cold
kubectl scale -n $NS deploy/home-timeline-service --replicas=3 >/dev/null
kubectl rollout restart -n $NS deploy/home-timeline-service >/dev/null
kubectl rollout status -n $NS deploy/home-timeline-service --timeout=180s >/dev/null

echo "[$(ts)] === warmup 90s @ 300 RPS Zipf=1.3 ==="
ssh -n "$SSH_TARGET" "ZIPF_ALPHA=1.3 ZIPF_N=962 $REMOTE_WRK2 -t 8 -c 100 -d 90s -R 300 -s $REMOTE_WORKLOAD_SCRIPT $TARGET_ENDPOINT > /tmp/sn_${COND}_${TRIAL}_warm.log 2>&1"

echo "[$(ts)] === observe phase: 25s pre + scale 3->4 + 120s post ==="
MARK=$(python3 -c "import time; print(time.time()+25)")

ssh -n "$SSH_TARGET" "ZIPF_ALPHA=1.3 ZIPF_N=962 $REMOTE_WRK2 -t 8 -c 100 -d 145s -L -R 300 -s $REMOTE_WORKLOAD_SCRIPT $TARGET_ENDPOINT > /tmp/sn_${COND}_${TRIAL}_obs.log 2>&1" &
LOAD_PID=$!

sleep 25
echo "[$(ts)] === SCALE 3 -> 4 ==="
kubectl scale -n $NS deploy/home-timeline-service --replicas=4 >/dev/null
NEW_POD=$(kubectl get pods -n $NS -l io.kompose.service=home-timeline-service --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
echo "expected new pod: $NEW_POD"

wait $LOAD_PID
ssh -n "$SSH_TARGET" "cat /tmp/sn_${COND}_${TRIAL}_warm.log" > "$OUT/wrk2_warmup.log"
ssh -n "$SSH_TARGET" "cat /tmp/sn_${COND}_${TRIAL}_obs.log" > "$OUT/wrk2_observe.log"
echo "trigger=$MARK new_pod=$NEW_POD" > "$OUT/event_meta.txt"
echo "[$(ts)] === ${COND}/${TRIAL} done ==="
ls "$OUT"
