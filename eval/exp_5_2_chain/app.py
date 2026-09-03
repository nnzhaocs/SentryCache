"""ChainApp service for §5.2.2 depth-amplification experiment.

A single Flask container plays the role of any chain layer. The layer's
identity (LAYER index, total DEPTH) and its upstream Service hostname are
all driven by env vars, so the same image is reused for every layer.

Per-request behaviour:
  GET /req/<int:k>
    cached = redis.get(f"k{k}")          # per-pod L1
    if cached: return cached
    if LAYER < DEPTH:
        v = http.get(f"http://chain-layer-{LAYER+1}/req/{k}")
    else:
        time.sleep(BACKEND_MS / 1000)    # terminal layer hits "backend"
        v = f"backend_v_{k}"
    redis.set(f"k{k}", v); return v

Headers:
  X-From-Cache: 1 / 0           — cache outcome at this layer
  X-Pod-Name:    pod that served
  X-Layer:       LAYER (so the load gen knows which layer responded)
"""
import os
import time

from flask import Flask, make_response
import redis
import urllib.request

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
BACKEND_MS = int(os.environ.get("BACKEND_MS", "200"))
VALUE_BYTES = int(os.environ.get("VALUE_BYTES", "8192"))
POD_NAME = os.environ.get("POD_NAME", "anon")
LAYER = int(os.environ.get("LAYER", "1"))
DEPTH = int(os.environ.get("DEPTH", "1"))
NS_DOMAIN = os.environ.get("NS_DOMAIN", "sc-eval-chain.svc.cluster.local")

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False, socket_timeout=2)
PADDING = b"x" * VALUE_BYTES


def call_next(k):
    url = f"http://chain-layer-{LAYER+1}.{NS_DOMAIN}/key/{k}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "chain"})
        with urllib.request.urlopen(req, timeout=4) as resp:
            return resp.read()
    except Exception:
        return b""


@app.get("/key/<int:k>")
def req(k):
    cache_key = f"k{k}".encode()
    try:
        cached = r.get(cache_key)
    except Exception:
        cached = None

    if cached:
        resp = make_response(cached, 200)
        resp.headers["X-From-Cache"] = "1"
    else:
        if LAYER < DEPTH:
            v = call_next(k)
            if not v:
                v = f"err_{k}".encode()
        else:
            time.sleep(BACKEND_MS / 1000.0)
            v = f"backend_{k}_".encode() + PADDING[: VALUE_BYTES - 12]
        try:
            r.set(cache_key, v)
        except Exception:
            pass
        resp = make_response(v, 200)
        resp.headers["X-From-Cache"] = "0"

    resp.headers["X-Pod-Name"] = POD_NAME
    resp.headers["X-Layer"] = str(LAYER)
    resp.headers["Content-Type"] = "application/octet-stream"
    return resp


@app.get("/healthz")
def healthz():
    return {"layer": LAYER, "depth": DEPTH}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
