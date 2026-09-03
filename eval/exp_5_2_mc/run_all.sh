#!/usr/bin/env bash
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
NS=sc-eval
TRIAL_RUN=${SENTRYCACHE_ROOT}/eval/exp_5_2_mc/run_trial.sh
CONTROLLER=${SENTRYCACHE_ROOT}/controller/main.py
L2_IP=$(kubectl get svc -n $NS l2-metadata -o jsonpath='{.spec.clusterIP}')

start_controller() {
  local mode=$1
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
  local extra="--no-migration --no-version"
  [ "$mode" = "noprefetch" ] && extra="--no-prefetch --no-migration --no-version"
  L2_HOST="$L2_IP" SC_NAMESPACE=$NS \
    SC_LABEL_KEY=sc-deployment SC_LABEL_VALUE=cache-svc-mc \
    CACHE_BACKEND=memcached SIDECAR_PORT=11211 \
    PREFETCH_TOPN=500 PYTHONPATH=${SENTRYCACHE_ROOT}/controller \
    nohup python3 "$CONTROLLER" $extra > /tmp/controller_mc_${mode}.log 2>&1 &
  sleep 4
  echo "controller started ($mode, memcached backend)"
}

stop_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
}

for cond in stock sentrycache np; do
  case "$cond" in
    stock) stop_controller ;;
    sentrycache) start_controller default ;;
    np) start_controller noprefetch ;;
  esac
  for i in 1 2 3; do
    echo "===== ${cond}/${i} ====="
    bash "$TRIAL_RUN" "$cond" "$i" || echo "TRIAL FAILED ${cond}/${i} — continuing"
    sleep 5
  done
done

stop_controller
echo "ALL 9 MC TRIALS DONE"
