#!/usr/bin/env bash
# 32-node full WarmScale/SentryCache runner.
# It renders manifests with topologySpreadConstraints, runs Stock/WarmScale
# and ablation conditions, collects cluster state, and generates CSV/SVG output.
set -euo pipefail

unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVAL_DIR="$ROOT_DIR/eval"
NS="${NS:-sc-eval-32}"
# Defaults documented for static audit: START_REPLICAS=3, TARGET_REPLICAS=32.
START_REPLICAS="${START_REPLICAS:-3}"
TARGET_REPLICAS=32
SCALEIN_REPLICAS="${SCALEIN_REPLICAS:-16}"
TRIALS="${TRIALS:-3}"
RPS="${RPS:-80}"
ZIPF_ALPHA="${ZIPF_ALPHA:-0.9}"
ZIPF_N="${ZIPF_N:-5000}"
PREFETCH_TOPN="${PREFETCH_TOPN:-1500}"
RESULT_ROOT="${RESULT_ROOT:-$ROOT_DIR/results/32node_full_$(date -u +%Y%m%d_%H%M%S)}"

LOADGEN="$EVAL_DIR/exp_5_4_migration/loadgen.py"
ROLLING_PROBE="$EVAL_DIR/exp_5_3_rolling/probe.py"
RENDER="$EVAL_DIR/render_32node_manifests.py"
ANALYZE="$EVAL_DIR/analyze_32node_results.py"
CONTROLLER="$ROOT_DIR/controller/main.py"

ts() { date -u +%H:%M:%S; }

collect_cluster_state() {
  local out="$1"
  mkdir -p "$out"
  kubectl get nodes -o wide > "$out/nodes.txt" || true
  kubectl get pods -A -o wide > "$out/pods_all.txt" || true
  kubectl get deploy,svc -n "$NS" -o wide > "$out/workloads.txt" || true
}

apply_manifests() {
  echo "[$(ts)] rendering and applying 32-node manifests namespace=$NS"
  mkdir -p "$RESULT_ROOT/manifests"
  python3 "$RENDER" --namespace "$NS" --replicas "$TARGET_REPLICAS" --root "$ROOT_DIR" \
    > "$RESULT_ROOT/manifests/sc-eval-32.yaml"
  kubectl apply -f "$RESULT_ROOT/manifests/sc-eval-32.yaml"
  kubectl rollout status -n "$NS" deploy/l2-metadata --timeout=240s
}

stop_controller() {
  pkill -f "controller/main.py" 2>/dev/null || true
  sleep 1
}

start_controller() {
  local mode="$1"
  stop_controller
  local l2_ip
  l2_ip="$(kubectl get svc -n "$NS" l2-metadata -o jsonpath='{.spec.clusterIP}')"
  local extra=""
  case "$mode" in
    full) extra="" ;;
    noprefetch) extra="--no-prefetch --no-migration --no-version" ;;
    nomigrate) extra="--no-migration --no-prefetch --no-version" ;;
    noversion) extra="--no-version" ;;
    prefetchonly) extra="--no-migration --no-version" ;;
    *) echo "unknown controller mode $mode"; exit 2 ;;
  esac
  mkdir -p "$RESULT_ROOT/controller"
  echo "[$(ts)] starting controller mode=$mode L2=$l2_ip"
  L2_HOST="$l2_ip" SC_NAMESPACE="$NS" PREFETCH_TOPN="$PREFETCH_TOPN" \
    PYTHONPATH="$ROOT_DIR/controller" nohup python3 "$CONTROLLER" $extra \
    > "$RESULT_ROOT/controller/controller_${mode}_$(date -u +%H%M%S).log" 2>&1 &
  sleep 5
}

wait_deploy() {
  local deploy="$1"
  kubectl rollout status -n "$NS" "deploy/$deploy" --timeout=600s
}

reset_cache_svc() {
  local replicas="$1"
  kubectl scale -n "$NS" deploy/cache-svc --replicas="$replicas" >/dev/null
  kubectl rollout restart -n "$NS" deploy/cache-svc >/dev/null
  wait_deploy cache-svc
}

run_scaleout_trial() {
  local cond="$1"
  local trial="$2"
  local out="$RESULT_ROOT/scaleout/$cond/trial_$trial"
  mkdir -p "$out"
  echo "[$(ts)] scaleout $cond trial=$trial reset $START_REPLICAS -> $TARGET_REPLICAS"
  reset_cache_svc "$START_REPLICAS"
  python3 "$LOADGEN" --namespace "$NS" --label app=cache-svc --port 8080 \
    --duration 90 --rps "$RPS" --zipf-alpha "$ZIPF_ALPHA" --zipf-n "$ZIPF_N" \
    --out "$out" --label-out warmup
  local mark
  mark="$(python3 -c 'import time; print(time.time()+25)')"
  python3 "$LOADGEN" --namespace "$NS" --label app=cache-svc --port 8080 \
    --duration 205 --rps "$RPS" --zipf-alpha "$ZIPF_ALPHA" --zipf-n "$ZIPF_N" \
    --out "$out" --label-out observe --mark "$mark" &
  local load_pid=$!
  sleep 25
  kubectl scale -n "$NS" deploy/cache-svc --replicas="$TARGET_REPLICAS" >/dev/null
  wait "$load_pid"
  echo "condition=$cond trial=$trial start=$START_REPLICAS target=$TARGET_REPLICAS mark=$mark" > "$out/event_meta.txt"
}

run_scaleout() {
  for cond in stock sentrycache np; do
    case "$cond" in
      stock) stop_controller ;;
      sentrycache) start_controller prefetchonly ;;
      np) start_controller noprefetch ;;
    esac
    for trial in $(seq 1 "$TRIALS"); do
      run_scaleout_trial "$cond" "$trial" || echo "scaleout failed cond=$cond trial=$trial"
      collect_cluster_state "$RESULT_ROOT/state/scaleout_${cond}_${trial}"
    done
  done
  stop_controller
}

run_migration_trial() {
  local cond="$1"
  local scenario="$2"
  local trial="$3"
  local out="$RESULT_ROOT/migration/$cond/$scenario/trial_$trial"
  mkdir -p "$out"
  echo "[$(ts)] migration $cond $scenario trial=$trial reset to $TARGET_REPLICAS"
  reset_cache_svc "$TARGET_REPLICAS"
  python3 "$LOADGEN" --namespace "$NS" --label app=cache-svc --port 8080 \
    --duration 120 --rps "$RPS" --zipf-alpha "$ZIPF_ALPHA" --zipf-n "$ZIPF_N" \
    --out "$out" --label-out warmup
  local mark
  mark="$(python3 -c 'import time; print(time.time()+25)')"
  python3 "$LOADGEN" --namespace "$NS" --label app=cache-svc --port 8080 \
    --duration 205 --rps "$RPS" --zipf-alpha "$ZIPF_ALPHA" --zipf-n "$ZIPF_N" \
    --out "$out" --label-out observe --mark "$mark" &
  local load_pid=$!
  sleep 25
  case "$scenario" in
    scalein)
      kubectl scale -n "$NS" deploy/cache-svc --replicas="$SCALEIN_REPLICAS" >/dev/null
      ;;
    crash)
      kubectl get pods -n "$NS" -l app=cache-svc --field-selector=status.phase=Running \
        -o name | head -4 | xargs -r kubectl delete -n "$NS" --force --grace-period=0
      ;;
    *) echo "bad scenario $scenario"; exit 2 ;;
  esac
  wait "$load_pid"
  echo "condition=$cond scenario=$scenario trial=$trial target=$TARGET_REPLICAS scalein=$SCALEIN_REPLICAS mark=$mark" > "$out/event_meta.txt"
}

run_migration() {
  for cond in stock sentrycache nm; do
    case "$cond" in
      stock) stop_controller ;;
      sentrycache) start_controller full ;;
      nm) start_controller nomigrate ;;
    esac
    for scenario in scalein crash; do
      for trial in $(seq 1 "$TRIALS"); do
        run_migration_trial "$cond" "$scenario" "$trial" || echo "migration failed cond=$cond scenario=$scenario trial=$trial"
        collect_cluster_state "$RESULT_ROOT/state/migration_${cond}_${scenario}_${trial}"
      done
    done
  done
  stop_controller
}

reset_timeline() {
  kubectl exec -n "$NS" deploy/shared-cache -- redis-cli -n 0 FLUSHDB >/dev/null || true
  kubectl exec -n "$NS" deploy/shared-cache -- redis-cli -n 1 FLUSHDB >/dev/null || true
  kubectl scale -n "$NS" deploy/timeline --replicas="$TARGET_REPLICAS" >/dev/null
  kubectl set env -n "$NS" deploy/timeline VERSION=v1 REDIS_DB=0 >/dev/null
  kubectl label -n "$NS" deploy/timeline sc-version=v1 --overwrite >/dev/null || true
  kubectl rollout restart -n "$NS" deploy/timeline >/dev/null
  wait_deploy timeline
}

run_rolling_trial() {
  local cond="$1"
  local trial="$2"
  local out="$RESULT_ROOT/rolling/$cond/trial_$trial"
  mkdir -p "$out"
  echo "[$(ts)] rolling $cond trial=$trial replicas=$TARGET_REPLICAS"
  reset_timeline
  local svc_ip
  svc_ip="$(kubectl get svc -n "$NS" timeline -o jsonpath='{.spec.clusterIP}')"
  python3 "$ROLLING_PROBE" --url "http://$svc_ip" --duration 45 --rps "$RPS" --out "$out" --label warmup || true
  case "$cond" in
    stock|nv)
      kubectl set env -n "$NS" deploy/timeline VERSION=v2 REDIS_DB=0 >/dev/null
      kubectl label -n "$NS" deploy/timeline sc-version=v2 --overwrite >/dev/null || true
      ;;
    sentrycache)
      kubectl set env -n "$NS" deploy/timeline VERSION=v2 REDIS_DB=1 >/dev/null
      kubectl label -n "$NS" deploy/timeline sc-version=v2 --overwrite >/dev/null || true
      ;;
  esac
  python3 "$ROLLING_PROBE" --url "http://$svc_ip" --duration 120 --rps "$RPS" --out "$out" --label observe || true
}

run_rolling() {
  for cond in stock sentrycache nv; do
    case "$cond" in
      stock) stop_controller ;;
      sentrycache) start_controller full ;;
      nv) start_controller noversion ;;
    esac
    for trial in $(seq 1 "$TRIALS"); do
      run_rolling_trial "$cond" "$trial" || echo "rolling failed cond=$cond trial=$trial"
      collect_cluster_state "$RESULT_ROOT/state/rolling_${cond}_${trial}"
    done
  done
  stop_controller
}

main() {
  mkdir -p "$RESULT_ROOT"
  echo "RESULT_ROOT=$RESULT_ROOT"
  apply_manifests
  collect_cluster_state "$RESULT_ROOT/state/initial"
  run_scaleout
  run_migration
  run_rolling
  collect_cluster_state "$RESULT_ROOT/state/final"
  python3 "$ANALYZE" --results "$RESULT_ROOT"
  echo "[$(ts)] 32-node full WarmScale/SentryCache run complete: $RESULT_ROOT"
}

main "$@"
