#!/usr/bin/env bash
# §5.2.2 ChainApp orchestrator: 4 depths × 2 conditions × 3 trials = 24 trials.
set -e
: "${SENTRYCACHE_ROOT:?set SENTRYCACHE_ROOT to the cloned repository root}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
NS=sc-eval-chain
TRIAL_RUN=${SENTRYCACHE_ROOT}/eval/exp_5_2_chain/run_trial.sh
RENDER=${SENTRYCACHE_ROOT}/eval/exp_5_2_chain/render_manifests.py
APP=${SENTRYCACHE_ROOT}/eval/exp_5_2_chain/app.py
CONTROLLER=${SENTRYCACHE_ROOT}/controller/main.py
L2_IP=$(kubectl get svc -n sc-eval l2-metadata -o jsonpath='{.spec.clusterIP}')

start_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
  L2_HOST="$L2_IP" SC_NAMESPACE=$NS \
    SC_LABEL_KEY=sc-deployment SC_LABEL_VALUE=chain-layer-1 \
    PREFETCH_TOPN=500 nohup python3 "$CONTROLLER" --no-migration --no-version \
      > /tmp/controller_chain.log 2>&1 &
  sleep 4
}

stop_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
}

deploy_depth() {
  local d=$1
  echo "=== deploy depth=$d ==="
  kubectl create namespace $NS --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl create configmap chain-code -n $NS --from-file=app.py=$APP --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  python3 "$RENDER" $d | kubectl apply -f - >/dev/null
  # Wait for all $d layer deployments to be Ready
  for layer in $(seq 1 $d); do
    kubectl rollout status -n $NS deploy/chain-layer-${layer} --timeout=240s >/dev/null
  done
  echo "depth=$d ready"
}

teardown_depth() {
  echo "=== teardown ==="
  kubectl delete deploy -n $NS --all --wait=false >/dev/null 2>&1 || true
  kubectl delete svc -n $NS --all --wait=false >/dev/null 2>&1 || true
  sleep 5
}

for d in 1 3; do
  echo "============================="
  echo "===== depth=$d setup ====="
  echo "============================="
  deploy_depth $d
  for cond in stock sentrycache; do
    case "$cond" in
      stock) stop_controller ;;
      sentrycache) start_controller ;;
    esac
    for i in 1 2 3; do
      echo "===== d=$d ${cond}/${i} ====="
      bash "$TRIAL_RUN" "$d" "$cond" "$i" || echo "TRIAL FAILED d=$d ${cond}/${i} — continuing"
      sleep 3
    done
  done
  teardown_depth
done

stop_controller
echo "ALL 24 CHAIN TRIALS DONE"
