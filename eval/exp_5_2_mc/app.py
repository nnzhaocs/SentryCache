"""cache-svc app — memcached variant (§5.2 backend-agnostic validation).

Mirrors eval/exp_5_4_migration/app.py exactly, but talks to a memcached
sidecar on port 11211 instead of a Redis sidecar on 6379. Same cache-aside
semantics, same 200 ms backend miss, same 8 KB value padding so the
per-pod cache pressure is identical to the Redis variant.
"""
import os
import time

from flask import Flask, make_response
from pymemcache.client.base import Client as MCClient

CACHE_HOST = os.environ.get("CACHE_HOST", "127.0.0.1")
CACHE_PORT = int(os.environ.get("CACHE_PORT", "11211"))
BACKEND_MS = int(os.environ.get("BACKEND_MS", "200"))
VALUE_BYTES = int(os.environ.get("VALUE_BYTES", "8192"))
POD_NAME = os.environ.get("POD_NAME", "anon")

app = Flask(__name__)
PADDING = b"x" * VALUE_BYTES


def mc():
    return MCClient((CACHE_HOST, CACHE_PORT),
                    connect_timeout=2, timeout=2)


@app.get("/key/<int:k>")
def get_key(k):
    cache_key = f"k{k}".encode()
    cached = None
    try:
        client = mc()
        cached = client.get(cache_key)
        client.close()
    except Exception:
        cached = None
    if cached:
        resp = make_response(cached, 200)
        resp.headers["X-From-Cache"] = "1"
    else:
        time.sleep(BACKEND_MS / 1000.0)
        v = f"val_{k}_".encode() + PADDING[: VALUE_BYTES - 8]
        try:
            client = mc()
            client.set(cache_key, v, expire=0)
            client.close()
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
