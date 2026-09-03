"""MuCache-style baseline for §5.2.

In-process LRU cache (cachetools) — NO sidecar Redis. New pod starts with
empty cache; lifecycle events have no cache awareness. Wire-compatible
with the existing §5.2/§5.4 loadgen (returns 8 KB body + X-From-Cache
header).

Per evaluation guide §4.3: 16 MB cache. With 8 KB values that maps to
~2000 entries (the spec's "8000 × 2KB" assumes 2 KB values; we use 8 KB
to stay apples-to-apples with §5.2.1's per-pod 8 MB / ~1000 × 8 KB
sidecar setup).
"""
import os
import threading
import time

from cachetools import LRUCache
from flask import Flask, make_response

MUCACHE_SIZE = int(os.environ.get("MUCACHE_SIZE", "2000"))
BACKEND_MS = int(os.environ.get("BACKEND_MS", "200"))
VALUE_BYTES = int(os.environ.get("VALUE_BYTES", "8192"))
POD_NAME = os.environ.get("POD_NAME", "anon")

_cache = LRUCache(maxsize=MUCACHE_SIZE)
_lock = threading.Lock()
PADDING = b"x" * VALUE_BYTES

app = Flask(__name__)


@app.get("/key/<int:k>")
def get_key(k):
    cache_key = f"k{k}"
    with _lock:
        cached = _cache.get(cache_key)
    if cached is not None:
        resp = make_response(cached, 200)
        resp.headers["X-From-Cache"] = "1"
    else:
        time.sleep(BACKEND_MS / 1000.0)
        v = f"val_{k}_".encode() + PADDING[: VALUE_BYTES - 8]
        with _lock:
            _cache[cache_key] = v
        resp = make_response(v, 200)
        resp.headers["X-From-Cache"] = "0"
    resp.headers["X-Pod-Name"] = POD_NAME
    resp.headers["Content-Type"] = "application/octet-stream"
    return resp


@app.get("/healthz")
def healthz():
    return {"ok": True, "size": len(_cache), "max": MUCACHE_SIZE}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
