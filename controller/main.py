"""SentryCache central controller (simplified, paper §IV).

Single Python process. Runs on the controller node. Watches K8s for pod lifecycle and
deployment image changes; reaches into per-pod sidecar Redis instances via
direct pod-IP connections to do DUMP/RESTORE; maintains an L2 metadata Redis
that records the replica set Sk for each cached key.

The controller does NOT intercept RESP traffic. Application pods talk to their
sidecar Redis on localhost:6379 unmodified. SentryCache's prefetch / migration
/ version isolation runs out-of-band on lifecycle events.

Design note: the "version tag" abstraction in the paper is implemented here as
a Redis DB number: v1 → DB 0, v2 → DB 1. Per-key prefix tagging gives the same
isolation but is more invasive on the application; DB selection is functionally
equivalent and lets us run with stock Flask.
"""

from __future__ import annotations

import hashlib
import os
# Strip proxy env vars before importing the kubernetes client — the cluster
# API uses self-signed certs and a corporate HTTPS_PROXY breaks verification.
for _v in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
    os.environ.pop(_v, None)

import argparse
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import redis
from kubernetes import client, config, watch

LOG = logging.getLogger("sentrycache")

# ---------- L2 schema -------------------------------------------------------
# Every key is namespaced by deployment so we can run experiments side by side
# without their replica-sets colliding.
#
#   Sk hash:           replicas:{deployment}:{cache_key}  -> {pod_name: ts}
#   pod IP map:        pods:{deployment}                  -> {pod_name: pod_ip}
#   pod version map:   versions:{deployment}              -> {pod_name: ver_tag}

# ---------- helpers ---------------------------------------------------------

L2_HOST = os.environ.get("L2_HOST", "l2-metadata.sc-eval.svc.cluster.local")
L2_PORT = int(os.environ.get("L2_PORT", "6379"))
NAMESPACE = os.environ.get("SC_NAMESPACE", "sc-eval")
# Label key whose value identifies the deployment we manage. Default
# `sc-deployment` matches our self-built workloads; for the SocialNetwork
# §5.2 test we set this to `io.kompose.service` so we can pick up DSB pods
# without modifying their labels.
SC_LABEL_KEY = os.environ.get("SC_LABEL_KEY", "sc-deployment")
# Optional restriction: only act on pods whose SC_LABEL_KEY value matches
# this exact string. Empty = act on every pod that has SC_LABEL_KEY at all.
SC_LABEL_VALUE = os.environ.get("SC_LABEL_VALUE", "")
# Optional version label override for §IV-D. For sc-eval defaults this is
# `sc-version`; for DSB SocialNetwork it doesn't matter (single version).
SC_VERSION_KEY = os.environ.get("SC_VERSION_KEY", "sc-version")
SIDECAR_PORT = int(os.environ.get("SIDECAR_PORT", "6379"))
# CACHE_BACKEND: "redis" (default) or "memcached". Changes the prefetch /
# migration paths to use ASCII memcached protocol via a hand-rolled client
# (see memcache_backend.py) instead of redis-py DUMP/RESTORE.
CACHE_BACKEND = os.environ.get("CACHE_BACKEND", "redis")
PREFETCH_TOPN = int(os.environ.get("PREFETCH_TOPN", "500"))
MIGRATE_GRACE_S = int(os.environ.get("MIGRATE_GRACE_S", "25"))
R_MIN = int(os.environ.get("R_MIN", "1"))


@dataclass
class PodInfo:
    name: str
    ip: str
    deployment: str
    version: str = "v1"
    ready: bool = False


def l2() -> redis.Redis:
    return redis.Redis(host=L2_HOST, port=L2_PORT, decode_responses=False, socket_timeout=2)


def sidecar_for(pod: PodInfo, db: int = 0) -> redis.Redis:
    return redis.Redis(host=pod.ip, port=SIDECAR_PORT, db=db,
                       decode_responses=False, socket_timeout=2)


# ---------- L2 ops ----------------------------------------------------------

def l2_register_pod(pod: PodInfo):
    r = l2()
    r.hset(f"pods:{pod.deployment}", pod.name, pod.ip)
    r.hset(f"versions:{pod.deployment}", pod.name, pod.version)


def l2_remove_pod(deployment: str, pod_name: str):
    r = l2()
    r.hdel(f"pods:{deployment}", pod_name)
    r.hdel(f"versions:{deployment}", pod_name)
    # Sweep replicas:* sets — could be slow at scale; OK for our experiments.
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor, match=f"replicas:{deployment}:*", count=500)
        for key in batch:
            r.hdel(key, pod_name)
        if cursor == 0:
            break


def l2_register_key(deployment: str, key: bytes, pod_name: str):
    r = l2()
    r.hset(f"replicas:{deployment}:{key.decode(errors='replace')}",
           pod_name, str(int(time.time())))


def l2_get_sk(deployment: str, key: bytes) -> dict[str, str]:
    r = l2()
    raw = r.hgetall(f"replicas:{deployment}:{key.decode(errors='replace')}")
    return {k.decode(): v.decode() for k, v in raw.items()}


def l2_pods(deployment: str) -> list[PodInfo]:
    r = l2()
    pods_raw = r.hgetall(f"pods:{deployment}")
    versions_raw = r.hgetall(f"versions:{deployment}")
    out = []
    for name_b, ip_b in pods_raw.items():
        out.append(PodInfo(
            name=name_b.decode(),
            ip=ip_b.decode(),
            deployment=deployment,
            version=versions_raw.get(name_b, b"v1").decode(),
        ))
    return out


# ---------- sidecar ops -----------------------------------------------------

def transfer_keys(source: PodInfo, target: PodInfo, keys: list[bytes],
                  source_db: int = 0, target_db: int = 0) -> int:
    """DUMP from source / RESTORE on target. Returns count successful."""
    src = sidecar_for(source, db=source_db)
    dst = sidecar_for(target, db=target_db)
    n = 0
    for k in keys:
        try:
            payload = src.dump(k)
            if payload is None:
                continue
            dst.restore(k, 0, payload, replace=True)
            n += 1
        except Exception as e:
            LOG.debug("transfer %r %s -> %s failed: %s", k, source.name, target.name, e)
    return n


def lfu_snapshot(pod: PodInfo, db: int = 0, max_keys: int = 5000) -> list[tuple[bytes, int, int]]:
    """Return [(key, freq, size)] from the pod's sidecar.

    Redis backend: SCAN + OBJECT FREQ + MEMORY USAGE.
    Memcached backend: stats items + stats cachedump (no per-key freq, so
        we synthesise freq=1 across the board and let the size term in
        score_pre = freq / size pick smaller items first — equivalent to
        "fit more keys in the prefetch budget").
    """
    if CACHE_BACKEND == "memcached":
        return _memcached_snapshot(pod, max_keys=max_keys)
    r = sidecar_for(pod, db=db)
    out: list[tuple[bytes, int, int]] = []
    cursor = 0
    seen = 0
    while True:
        cursor, batch = r.scan(cursor, count=200)
        for k in batch:
            try:
                # OBJECT FREQ requires maxmemory-policy = allkeys-lfu / volatile-lfu
                freq = r.execute_command("OBJECT", "FREQ", k)
                size = r.execute_command("MEMORY", "USAGE", k) or 1
                out.append((k, int(freq), int(size)))
                seen += 1
                if seen >= max_keys:
                    return out
            except Exception:
                continue
        if cursor == 0:
            break
    return out


def _memcached_snapshot(pod: "PodInfo", max_keys: int = 5000) -> list[tuple[bytes, int, int]]:
    from memcache_backend import MemcachedClient
    mc = MemcachedClient(pod.ip, port=SIDECAR_PORT)
    try:
        items = mc.list_keys_with_size(max_keys=max_keys)
    finally:
        mc.close()
    # synthesise freq = 1; score_pre's size term still differentiates.
    return [(k, 1, max(1, size)) for k, size in items]


def memcached_transfer(source: "PodInfo", target: "PodInfo",
                      keys: list[bytes]) -> int:
    """get/set transfer for memcached backend. Returns count of successful moves."""
    from memcache_backend import MemcachedClient
    src = MemcachedClient(source.ip, port=SIDECAR_PORT)
    dst = MemcachedClient(target.ip, port=SIDECAR_PORT)
    n = 0
    try:
        for k in keys:
            try:
                got = src.get_with_meta(k)
                if got is None:
                    continue
                value, flags = got
                if dst.set_with_meta(k, value, flags=flags, exptime=0):
                    n += 1
            except Exception:
                continue
    finally:
        src.close()
        dst.close()
    return n


# ---------- lifecycle handlers ---------------------------------------------

@dataclass
class Controller:
    enable_prefetch: bool = True
    enable_migration: bool = True
    enable_version: bool = True
    deployments: dict[str, dict] = field(default_factory=dict)
    pods: dict[str, PodInfo] = field(default_factory=dict)  # by pod_name
    _stop: bool = False

    # Receive a pod transition (Pending -> Scheduled, Scheduled -> Ready, etc.).
    def on_pod_event(self, pod_obj):
        meta = pod_obj.metadata
        if not meta or not meta.namespace == NAMESPACE:
            return
        labels = meta.labels or {}
        if SC_LABEL_KEY not in labels:
            return  # only manage opted-in deployments
        deployment = labels[SC_LABEL_KEY]
        if SC_LABEL_VALUE and deployment != SC_LABEL_VALUE:
            return  # caller pinned us to a single deployment
        version = labels.get(SC_VERSION_KEY, "v1")
        ip = pod_obj.status.pod_ip if pod_obj.status else None
        if not ip:
            return
        # Pod is Ready iff every container reports ready=True. This mirrors
        # the load generator's containerStatuses[*].ready filter, so prefetch
        # fires at the same instant the loadgen starts routing traffic to the
        # new pod — avoiding the K3s overlay routing-propagation race where
        # status.pod_ip is set before the IP becomes routable from the controller node.
        css = pod_obj.status.container_statuses or []
        ready = bool(css) and all(cs.ready for cs in css)

        info = PodInfo(name=meta.name, ip=ip, deployment=deployment,
                       version=version, ready=ready)
        prev = self.pods.get(meta.name)
        self.pods[meta.name] = info
        l2_register_pod(info)

        # Prefetch on the not-Ready -> Ready transition (the first time the
        # pod becomes routable). Skip if we missed the transition entirely
        # (prev was already Ready) or it just got its IP and is still booting
        # (prev is None or not-Ready, current still not-Ready).
        was_ready = bool(prev) and prev.ready
        just_became_ready = ready and not was_ready
        if self.enable_prefetch and just_became_ready:
            siblings = [p for p in self.pods.values()
                        if p.deployment == deployment and p.name != meta.name
                        and p.ready and p.version == version]
            if siblings:
                threading.Thread(
                    target=self._prefetch_into,
                    args=(info, siblings),
                    daemon=True,
                ).start()

    def on_pod_terminating(self, pod_name: str):
        info = self.pods.get(pod_name)
        if not info:
            return
        if self.enable_migration:
            self._migrate_out(info)
        l2_remove_pod(info.deployment, pod_name)
        self.pods.pop(pod_name, None)

    # ---------- §5.2 prefetch ----------
    def _prefetch_into(self, target: PodInfo, siblings: list[PodInfo]):
        t0 = time.time()
        # K3s overlay routing for a freshly-Ready pod's IP can take a few
        # extra seconds to propagate to the controller node, especially under wrk2
        # load contention. A 3 s settle delay before the first ping markedly
        # increases prefetch success rate without measurably eating into
        # the post-Ready window the loadgen uses.
        time.sleep(3.0)
        target_up = False
        for _ in range(60):
            try:
                if CACHE_BACKEND == "memcached":
                    from memcache_backend import MemcachedClient
                    mc = MemcachedClient(target.ip, port=SIDECAR_PORT)
                    if mc.ping():
                        target_up = True
                        mc.close()
                        break
                    mc.close()
                else:
                    sidecar_for(target).ping()
                    target_up = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if not target_up:
            LOG.warning("prefetch %s: target sidecar never came up", target.name)
            return

        # Compute the new hash ring (sibling IPs + target IP, sorted) so we
        # only prefetch keys that will route to this pod once the load
        # generator picks it up. Without this filter we'd transfer hot keys
        # that will route to *other* survivors and the prefetch is wasted.
        ring = sorted([target.ip] + [s.ip for s in siblings])
        new_idx = ring.index(target.ip)
        n_total = len(ring)

        try:
            snap = lfu_snapshot(siblings[0], db=0, max_keys=5000)
        except Exception as e:
            LOG.warning("lfu_snapshot failed on %s: %s", siblings[0].name, e)
            return
        if not snap:
            LOG.info("prefetch skip — sibling has no keys")
            return

        def hash_idx(key_bytes: bytes) -> int:
            s = key_bytes.decode("utf-8", errors="replace")
            try:
                if s.startswith("k"):
                    h = int(hashlib.md5(s[1:].encode()).hexdigest()[:8], 16)
                else:
                    h = int(hashlib.md5(s.encode()).hexdigest()[:8], 16)
            except Exception:
                return -1
            return h % n_total

        # Memcached path uses the same hash filter as Redis so the
        # backend-agnostic prefetch lands keys exactly where post-event
        # routing will look for them. (Earlier we omitted the filter on the
        # memcached path; that left ~75% of the prefetch budget loaded onto
        # the wrong slot. Without parity here, T_warmup can't drop because
        # the new pod's slot keys are mostly absent from its prefetched set.)
        if CACHE_BACKEND == "memcached":
            mc_filtered = [(k, f, sz) for k, f, sz in snap if hash_idx(k) == new_idx]
            mc_filtered.sort(key=lambda kfs: kfs[1] / max(1, kfs[2]), reverse=True)
            topn = mc_filtered[:PREFETCH_TOPN]
            if not topn:
                LOG.info("prefetch[mc] %s: no keys hash to slot %d/%d (sibling cache empty?)",
                         target.name, new_idx, n_total)
                return
            ok = memcached_transfer(siblings[0], target, [k for k, _, _ in topn])
            for k, _, _ in topn[:ok]:
                l2_register_key(target.deployment, k, target.name)
            LOG.info("prefetch[mc] %s <- %s: %d/%d keys (slot=%d/%d) in %.2fs",
                     target.name, siblings[0].name, ok, len(topn),
                     new_idx, n_total, time.time() - t0)
            return

        filtered = [(k, f, sz) for k, f, sz in snap if hash_idx(k) == new_idx]
        filtered.sort(key=lambda kfs: kfs[1] / max(1, kfs[2]), reverse=True)
        topn = filtered[:PREFETCH_TOPN]
        if not topn:
            LOG.info("prefetch %s: no keys hash to slot %d/%d (sibling cache empty?)",
                     target.name, new_idx, n_total)
            return
        ok = transfer_keys(siblings[0], target, [k for k, _, _ in topn], 0, 0)
        for k, _, _ in topn[:ok]:
            l2_register_key(target.deployment, k, target.name)
        LOG.info("prefetch %s <- %s: %d/%d keys (slot=%d/%d) in %.2fs",
                 target.name, siblings[0].name, ok, len(topn),
                 new_idx, n_total, time.time() - t0)

    # ---------- §5.4 migration ----------
    def _migrate_out(self, leaving: PodInfo):
        t0 = time.time()
        # Pick survivors with stable IP order so the hash-pick matches the
        # load generator's pod ordering (sorted IPs).
        survivors = sorted(
            [p for p in self.pods.values()
             if p.deployment == leaving.deployment
             and p.name != leaving.name and p.ready],
            key=lambda p: p.ip,
        )
        if not survivors:
            LOG.info("migrate %s: no survivors, skipping", leaving.name)
            return
        try:
            snap = lfu_snapshot(leaving, db=0, max_keys=10000)
        except Exception as e:
            LOG.warning("migrate snapshot failed on %s: %s", leaving.name, e)
            return
        scored = []
        for k, freq, size in snap:
            sk = l2_get_sk(leaving.deployment, k)
            sk_after = {p for p in sk if p != leaving.name}
            mandatory = len(sk_after) < R_MIN
            score = freq / (max(1, len(sk_after) * size))
            scored.append((mandatory, score, k))
        scored.sort(key=lambda x: (not x[0], -x[1]))  # mandatory first, then high score

        deadline = t0 + MIGRATE_GRACE_S
        ok = 0
        # Match the load generator: hash(numeric_suffix(key)) % len(survivors)
        # so that each migrated key lands on the pod where post-event hash
        # routing will look for it.
        def hash_target(key_bytes: bytes) -> "PodInfo":
            s = key_bytes.decode("utf-8", errors="replace")
            try:
                if s.startswith("k"):
                    h = int(hashlib.md5(s[1:].encode()).hexdigest()[:8], 16)
                else:
                    h = int(hashlib.md5(s.encode()).hexdigest()[:8], 16)
            except Exception:
                h = 0
            return survivors[h % len(survivors)]

        for mand, score, k in scored:
            if time.time() >= deadline:
                break
            receiver = hash_target(k)
            n = transfer_keys(leaving, receiver, [k])
            if n:
                l2_register_key(leaving.deployment, k, receiver.name)
                ok += 1
        LOG.info("migrate %s: %d / %d keys in %.2fs (survivors=%d)",
                 leaving.name, ok, len(scored), time.time() - t0,
                 len(survivors))

    # ---------- §5.3 version ----------
    # We don't intercept RESP, so the controller's "version" action is just to
    # observe rolling updates and ensure new pods register with their version
    # label. The application uses REDIS_DB env var to actually isolate caches.
    def on_version_change(self, deployment: str, new_version: str):
        LOG.info("version change deployment=%s -> %s", deployment, new_version)


# ---------- watch loop ------------------------------------------------------

def watch_pods(c: Controller):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    while not c._stop:
        try:
            for event in w.stream(v1.list_namespaced_pod, namespace=NAMESPACE,
                                  timeout_seconds=60):
                p = event["object"]
                etype = event["type"]
                if etype == "DELETED":
                    c.on_pod_terminating(p.metadata.name)
                else:
                    c.on_pod_event(p)
                    if p.metadata.deletion_timestamp:
                        c.on_pod_terminating(p.metadata.name)
        except Exception as e:
            LOG.warning("watch error: %s", e)
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-prefetch", action="store_true")
    parser.add_argument("--no-migration", action="store_true")
    parser.add_argument("--no-version", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    c = Controller(
        enable_prefetch=not args.no_prefetch,
        enable_migration=not args.no_migration,
        enable_version=not args.no_version,
    )
    LOG.info("starting controller ns=%s prefetch=%s migration=%s version=%s",
             NAMESPACE, c.enable_prefetch, c.enable_migration, c.enable_version)
    try:
        watch_pods(c)
    except KeyboardInterrupt:
        c._stop = True
        LOG.info("stopping")


if __name__ == "__main__":
    main()
