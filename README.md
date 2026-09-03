# SentryCache

SentryCache is an experimental, out-of-band cache lifecycle controller for Kubernetes. The implementation watches opted-in pods, maintains replica metadata in Redis, and performs cache prefetch or migration when pod lifecycle events are observed. It supports Redis and a limited Memcached path used by the evaluation harness.

This public tree is a compact research artifact. It intentionally excludes manuscripts, internal reports, host configuration, credentials, logs, generated figures, and archive files.

## Repository layout

- `controller/`: the Python controller and cache-transfer helpers.
- `sidecar/`: Lua helpers and reference RESP proxies in Go and Python.
- `manifests/`: scheduler-neutral L2 and example workload manifests.
- `eval/`: experiment runners, workload services, analysis, and SVG generation tools.
- `data/raw_data/`: archived CSV inputs used to validate reported aggregates.
- `data/derive_stats.py`: recomputes aggregate statistics from the archived CSV files.

## Requirements

- Python 3.10 or newer
- Kubernetes with `kubectl` configured
- Redis 7 or a compatible Redis deployment
- Go 1.22 or newer to rebuild the Go RESP proxy
- wrk2 and DeathStarBench for the SocialNetwork and HotelReservation runners

Install Python dependencies with:

```bash
python -m pip install -r requirements.txt
```

## Deploying the controller and sidecar

The most complete deployment path is the manifest renderer used by the 32-node harness:

```bash
python eval/render_32node_manifests.py \
  --namespace sc-eval \
  --replicas 4 \
  --root . > /tmp/sentrycache.yaml
kubectl apply -f /tmp/sentrycache.yaml
```

The controller acts only on pods carrying the label selected by `SC_LABEL_KEY` (default: `sc-deployment`). Important settings include `L2_HOST`, `L2_PORT`, `SC_NAMESPACE`, `SC_LABEL_VALUE`, `SC_VERSION_KEY`, `SIDECAR_PORT`, `CACHE_BACKEND`, `PREFETCH_TOPN`, `MIGRATE_GRACE_S`, and `R_MIN`.

The checked-in implementation starts prefetch after a pod becomes Ready, waits three seconds before probing the target cache, and uses the first eligible sibling as the transfer source. These details are stated explicitly so that this artifact is not mistaken for a broader design specification.

## Running evaluation scripts

Most local Kubernetes runners require these paths:

```bash
export SENTRYCACHE_ROOT="$PWD"
export RESULT_ROOT=/path/to/writable/results
```

DeathStarBench runners may additionally require:

```bash
export DEATHSTARBENCH_ROOT=/path/to/DeathStarBench
export LOADGEN_SSH_HOST=load-generator.example.org
export LOADGEN_SSH_USER=ubuntu
export TARGET_ENDPOINT=http://service.example.org:30080
export REMOTE_WRK2=/usr/local/bin/wrk
export REMOTE_WORKLOAD_SCRIPT=/path/on/load-generator/workload.lua
```

The rolling-update script accepts `PROBE_URL`; its default is `http://127.0.0.1:18080` for a local port-forward.

Review each runner before use. The scripts reset caches, restart deployments, and scale workloads inside the namespace selected by the script or its environment variables.

## Reproducing figures and aggregates

The 32-node result pipeline is:

```bash
python eval/analyze_32node_results.py --help
python eval/generate_32node_paper_figures.py --help
```

To recompute aggregate values from the included archived inputs:

```bash
python data/derive_stats.py
```

## Data provenance and limitations

Files in `data/raw_data/` are archived per-trial or per-time-window CSV inputs retained for result checking. They are not complete per-request traces. The unavailable low-level request logs are not represented as if they were public, and the projected-workload table is excluded from this release.

## License

No open-source license has been selected. Choose and add a license before public release if redistribution or reuse should be permitted.