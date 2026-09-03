#!/usr/bin/env bash
# Orchestrate all 18 trials for §5.4 (3 conditions × 2 scenarios × 3 trials).
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
NS=sc-eval
TRIAL_RUN=${SENTRYCACHE_ROOT}/eval/exp_5_4_migration/run_trial.sh
CONTROLLER=${SENTRYCACHE_ROOT}/controller/main.py
L2_IP=$(kubectl get svc -n $NS l2-metadata -o jsonpath='{.spec.clusterIP}')

start_controller() {
  local mode=$1   # default | nomigrate
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
  local extra=""
  if [ "$mode" = "nomigrate" ]; then extra="--no-migration --no-prefetch --no-version"; fi
  L2_HOST="$L2_IP" SC_NAMESPACE=$NS nohup python3 "$CONTROLLER" $extra > /tmp/controller_${mode}.log 2>&1 &
  sleep 4
  echo "controller started ($mode); pid=$!; L2_HOST=$L2_IP"
}

stop_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
}

for cond in stock sentrycache nm; do
  case "$cond" in
    stock) stop_controller ;;
    sentrycache) start_controller default ;;
    nm) start_controller nomigrate ;;
  esac

  for scen in scalein crash; do
    for i in 1 2 3; do
      echo "=================="
      echo "===== ${cond}/${scen}/${i} ====="
      echo "=================="
      bash "$TRIAL_RUN" "$cond" "$scen" "$i" || echo "TRIAL FAILED ${cond}/${scen}/${i} — continuing"
      sleep 5
    done
  done
done

stop_controller
echo "ALL 18 TRIALS DONE"
