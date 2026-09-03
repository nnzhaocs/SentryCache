"""§5.4 load generator with consistent hash routing.

Each cache key is deterministically hashed to a pod (pod_ips[hash(key) % N]).
This partitions cache state across pods — pod X holds a disjoint slice of
keys — which is what makes pod departure visibly cost cache state. The
generator queries K8s every 1 s to refresh its pod list, so when a pod
disappears the surviving pods absorb the orphaned slots and any GETs for
those keys miss (forcing a fresh backend round-trip).

Outputs the same JSONL + summary.json shape as before so parse_5_4.py keeps
working unchanged.
"""
import argparse
import asyncio
import hashlib
import json
import os
import random
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import aiohttp


def fetch_pod_ips(namespace: str, label: str) -> list[str]:
    """Returns the sorted IPs of pods that are Running AND have all their
    containers Ready. We re-poll every ~1 s in a background task. The Ready
    filter matters for §5.2: a freshly scheduled pod's IP shows up as Running
    well before its app container is listening on port 8080, and routing
    requests to that pod would log a flurry of connection-refused errors
    that aren't really cache misses."""
    try:
        env = os.environ.copy()
        for v in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
            env.pop(v, None)
        out = subprocess.check_output(
            ["kubectl", "get", "pods", "-n", namespace, "-l", label,
             "-o", "json"],
            env=env, stderr=subprocess.DEVNULL, timeout=2,
        ).decode()
        data = json.loads(out)
        ips = []
        for p in data.get("items", []):
            # drain-aware: skip pods that are terminating (have a
            # deletionTimestamp). During scale-in the pod stays Running/Ready
            # for its graceful-termination window, but routing keys to it logs
            # connection errors once the container exits. Excluding it removes
            # the false scale-in ERROR samples (see scale-in error-free rerun).
            meta = p.get("metadata", {}) or {}
            if meta.get("deletionTimestamp"):
                continue
            status = p.get("status", {}) or {}
            if status.get("phase") != "Running":
                continue
            ip = status.get("podIP")
            if not ip:
                continue
            css = status.get("containerStatuses") or []
            if css and all(c.get("ready") for c in css):
                ips.append(ip)
        return sorted(ips)
    except Exception:
        return []


def zipf(alpha: float, n: int) -> int:
    while True:
        u = random.random()
        x = int(n * u ** (1.0 / (1.0 - alpha)))
        if 1 <= x <= n:
            return x


def pick_pod(pod_ips: list[str], key_id: int) -> str | None:
    if not pod_ips:
        return None
    h = int(hashlib.md5(str(key_id).encode()).hexdigest()[:8], 16)
    return pod_ips[h % len(pod_ips)]


async def driver(session, port, pod_ref, alpha, n, t_end, samples, errors):
    while time.time() < t_end:
        k = zipf(alpha, n)
        ips = pod_ref["ips"]
        target = pick_pod(ips, k)
        t0 = time.time()
        if not target:
            errors.append((t0, "no_pods_known"))
            await asyncio.sleep(0.05)
            continue
        url = f"http://{target}:{port}/key/{k}"
        try:
            async with session.get(url,
                                   timeout=aiohttp.ClientTimeout(total=2)) as resp:
                await resp.read()
                hit = resp.headers.get("X-From-Cache") == "1"
                pod = resp.headers.get("X-Pod-Name", target)
                samples.append((t0, hit, pod, time.time() - t0))
        except Exception as e:
            # connection refused / timeout — count as a hard miss; the load
            # generator will rehash on the next call after the watcher updates
            # pod_ref["ips"].
            errors.append((t0, str(e)[:80]))
            samples.append((t0, False, "ERROR", time.time() - t0))


async def pod_watcher(pod_ref, namespace, label, t_end, interval=1.0):
    while time.time() < t_end:
        ips = fetch_pod_ips(namespace, label)
        if ips and ips != pod_ref["ips"]:
            print(f"[pod_watcher] @{int(time.time())} pods={ips}")
        if ips:
            pod_ref["ips"] = ips
        await asyncio.sleep(interval)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default="sc-eval")
    ap.add_argument("--label", default="app=cache-svc")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--rps", type=int, default=80)
    ap.add_argument("--zipf-alpha", type=float, default=0.9)
    ap.add_argument("--zipf-n", type=int, default=10000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label-out", default="run")
    ap.add_argument("--mark", type=float, default=None,
                    help="wall-clock timestamp of the trigger event (for plotting)")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    pod_ref = {"ips": fetch_pod_ips(args.namespace, args.label)}
    if not pod_ref["ips"]:
        print(f"WARN: no pods for label '{args.label}' in '{args.namespace}'")
    print(f"[start] pods={pod_ref['ips']}")

    samples: list[tuple[float, bool, str, float]] = []
    errors: list[tuple[float, str]] = []
    t_end = time.time() + args.duration

    n_drivers = max(1, args.rps // 8)
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200),
                                      timeout=timeout) as s:
        watcher = asyncio.create_task(pod_watcher(pod_ref, args.namespace,
                                                  args.label, t_end))
        await asyncio.gather(*[
            driver(s, args.port, pod_ref, args.zipf_alpha, args.zipf_n,
                   t_end, samples, errors)
            for _ in range(n_drivers)
        ])
        watcher.cancel()

    bucket_hits: dict[int, int] = defaultdict(int)
    bucket_total: dict[int, int] = defaultdict(int)
    pod_total: dict[str, int] = defaultdict(int)
    for ts, hit, pod, _lat in samples:
        sec = int(ts)
        bucket_total[sec] += 1
        if hit:
            bucket_hits[sec] += 1
        pod_total[pod] += 1

    seconds = sorted(bucket_total)
    trajectory = [{"sec_unix": sec,
                   "rps": bucket_total[sec],
                   "hits": bucket_hits[sec],
                   "hit_rate": bucket_hits[sec] / bucket_total[sec] if bucket_total[sec] else 0}
                  for sec in seconds]

    summary = {
        "label": args.label_out,
        "duration_s": args.duration,
        "target_rps": args.rps,
        "actual_total": len(samples),
        "actual_rps": len(samples) / args.duration if args.duration else 0,
        "errors": len(errors),
        "overall_hit_rate": (sum(1 for _t, h, *_ in samples if h) / len(samples))
                              if samples else 0,
        "pods_seen": dict(pod_total),
        "trajectory": trajectory,
        "mark_ts": args.mark,
        "starting_pod_ips": pod_ref["ips"],
    }
    (out_path / f"{args.label_out}.summary.json").write_text(json.dumps(summary, indent=2))
    with (out_path / f"{args.label_out}.jsonl").open("w") as f:
        for ts, hit, pod, lat in samples:
            f.write(json.dumps({"ts": ts, "hit": hit, "pod": pod, "lat": lat}) + "\n")
    print(json.dumps({"label": args.label_out, "actual_total": len(samples),
                      "errors": len(errors),
                      "overall_hit_rate": summary["overall_hit_rate"]}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
