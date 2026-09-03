"""Tiny KV service for §5.4 migration experiment.

Each pod has its own sidecar Redis (per-pod cache). The Service load-balances
requests across the 3 pods. Hit rate is reported per-request (X-From-Cache
header) so the load generator can build a 1-second-resolution hit-rate
trajectory around the departure event.

Backend "miss" cost is a 200 ms sleep + an 8 KB body (cache values are big
enough that an 8 MB sidecar holds ~1 000 keys; with a 50 000-key Zipf workload
the per-pod hot tails diverge so a pod loss really does cost cache state).
"""
import os
import time

from flask import Flask, make_response
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
BACKEND_MS = int(os.environ.get("BACKEND_MS", "200"))
VALUE_BYTES = int(os.environ.get("VALUE_BYTES", "8192"))
POD_NAME = os.environ.get("POD_NAME", "anon")

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False,
                socket_timeout=2)
PADDING = b"x" * VALUE_BYTES


@app.get("/key/<int:k>")
def get_key(k):
    cache_key = f"k{k}".encode()
    try:
        cached = r.get(cache_key)
    except Exception:
        cached = None
    if cached:
        resp = make_response(cached, 200)
        resp.headers["X-From-Cache"] = "1"
    else:
        time.sleep(BACKEND_MS / 1000.0)
        v = f"val_{k}_".encode() + PADDING[: VALUE_BYTES - 8]
        try:
            r.set(cache_key, v)
        except Exception:
            pass
        resp = make_response(v, 200)
        resp.headers["X-From-Cache"] = "0"
    resp.headers["X-Pod-Name"] = POD_NAME
    resp.headers["Content-Type"] = "application/octet-stream"
    return resp


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
