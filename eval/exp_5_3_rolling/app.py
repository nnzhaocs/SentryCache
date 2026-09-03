"""Tiny timeline service for §5.3 rolling-update experiment.

Behaviour:
  GET /timeline/<int:user_id>
    cache_key = f"u:{user_id}"
    if cache hit: return cached payload (verbatim)
    else: synthesise payload, cache it, return

  v1 payload schema:  {"user_id": .., "body": .., "author_id": ..}
  v2 payload schema:  {"user_id": .., "body": .., "author_id": .., "avatar_url": ..}

The version selector is the env var VERSION (v1 / v2). The cache DB is the env
var REDIS_DB (0 / 1). Stock condition: both v1 and v2 deployments use REDIS_DB=0
so v2 pods inherit v1's payloads (which lack avatar_url) — those are the
"stale reads". SentryCache condition: v2 uses REDIS_DB=1, fresh cache, clean.

Headers: every response carries X-Pod-Version so the probe can attribute stale
reads to the v2 pod that served them (vs. trailing v1 pod still in the rollout).
"""
import json
import os
import time

from flask import Flask, jsonify, make_response
import redis

VERSION = os.environ.get("VERSION", "v1")
REDIS_HOST = os.environ.get("REDIS_HOST", "shared-cache")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
POD_NAME = os.environ.get("POD_NAME", "anon")

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                decode_responses=True, socket_timeout=2)


def synthesise(uid: int) -> dict:
    payload = {
        "user_id": uid,
        "body": f"timeline body for user {uid}",
        "author_id": uid % 100,
    }
    if VERSION == "v2":
        payload["avatar_url"] = f"https://avatars.example/u/{uid}.png"
    return payload


@app.get("/timeline/<int:uid>")
def timeline(uid):
    key = f"u:{uid}"
    cached = r.get(key)
    if cached:
        body = cached
        from_cache = True
    else:
        # Tiny "backend" delay to make caching meaningful
        time.sleep(0.02)
        body = json.dumps(synthesise(uid))
        # Setex so leftover keys eventually disappear in long runs
        r.setex(key, 600, body)
        from_cache = False

    resp = make_response(body, 200)
    resp.headers["Content-Type"] = "application/json"
    resp.headers["X-Pod-Version"] = VERSION
    resp.headers["X-Pod-Name"] = POD_NAME
    resp.headers["X-From-Cache"] = "1" if from_cache else "0"
    return resp


@app.get("/healthz")
def healthz():
    return {"version": VERSION, "redis_db": REDIS_DB}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
