#!/usr/bin/env bash
# §5.2.1 SocialNetwork orchestrator.
# ENTRY: home-timeline-service (no -c) per-pod sidecar variant.
# Setup: switch Service selector to standalone variant; scale -c to 0;
# run controller targeting sc-demo / home-timeline-service.
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
NS=sc-demo
TRIAL_RUN=${SENTRYCACHE_ROOT}/eval/exp_5_2_socialnetwork/run_trial.sh
CONTROLLER=${SENTRYCACHE_ROOT}/controller/main.py
L2_IP=$(kubectl get svc -n sc-eval l2-metadata -o jsonpath='{.spec.clusterIP}')

start_controller() {
  local mode=$1   # default | noprefetch
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
  local extra="--no-migration --no-version"
  [ "$mode" = "noprefetch" ] && extra="--no-prefetch --no-migration --no-version"
  L2_HOST="$L2_IP" SC_NAMESPACE=$NS \
    SC_LABEL_KEY=io.kompose.service SC_LABEL_VALUE=home-timeline-service \
    PREFETCH_TOPN=500 nohup python3 "$CONTROLLER" $extra > /tmp/controller_sn_${mode}.log 2>&1 &
  sleep 4
  echo "controller started ($mode)"
}

stop_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
}

echo "=== save Service state + swap to standalone variant ==="
ORIG=$(kubectl get svc -n $NS home-timeline-service -o jsonpath='{.spec.selector}')
echo "$ORIG" > /tmp/sn_svc_selector_backup.txt
echo "saved original selector: $ORIG"
kubectl patch svc -n $NS home-timeline-service --type=merge \
  -p '{"spec":{"selector":{"io.kompose.service":"home-timeline-service"}}}' >/dev/null
echo "patched to standalone selector"

ORIG_C_REPLICAS=$(kubectl get deploy -n $NS home-timeline-service-c -o jsonpath='{.spec.replicas}')
echo "$ORIG_C_REPLICAS" > /tmp/sn_c_replicas_backup.txt
echo "scaling cluster variant ($ORIG_C_REPLICAS) -> 0"
kubectl scale -n $NS deploy/home-timeline-service-c --replicas=0 >/dev/null

trap 'echo restoring; kubectl patch svc -n '"$NS"' home-timeline-service --type=merge -p '"'"'{"spec":{"selector":{"io.kompose.service":"home-timeline-service-c"}}}'"'"' >/dev/null; kubectl scale -n '"$NS"' deploy/home-timeline-service-c --replicas='"$ORIG_C_REPLICAS"' >/dev/null; pkill -f controller/main.py 2>/dev/null || true' EXIT

for cond in stock sentrycache np; do
  case "$cond" in
    stock) stop_controller ;;
    sentrycache) start_controller default ;;
    np) start_controller noprefetch ;;
  esac
  for i in 1 2 3; do
    echo "=========================="
    echo "===== ${cond} / trial_${i} ====="
    bash "$TRIAL_RUN" "$cond" "$i" || echo "TRIAL FAILED ${cond}/${i} — continuing"
    sleep 5
  done
done

stop_controller
echo "=== restore ==="
kubectl patch svc -n $NS home-timeline-service --type=merge \
  -p '{"spec":{"selector":{"io.kompose.service":"home-timeline-service-c"}}}' >/dev/null
kubectl scale -n $NS deploy/home-timeline-service-c --replicas=$ORIG_C_REPLICAS >/dev/null
trap - EXIT
echo "ALL 9 SN TRIALS DONE; cluster restored"
