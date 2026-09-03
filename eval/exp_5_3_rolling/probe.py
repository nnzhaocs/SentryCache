"""§5.3 stale-read probe + lightweight load gen.

Pulls timelines via the Service ClusterIP; classifies each response as:
  v1            — pod served v1 (still in rollout) — neither fresh nor stale
  fresh-v2      — pod served v2, payload has avatar_url — what we want
  stale-v2      — pod served v2, payload missing avatar_url — broken read

Writes a CSV with one row per request and a final summary JSON.
"""
import argparse
import asyncio
import json
import random
import time
from pathlib import Path

import aiohttp


async def one_request(session, url, uid):
    t0 = time.time()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as resp:
            text = await resp.text()
            ver = resp.headers.get("X-Pod-Version", "?")
            pod = resp.headers.get("X-Pod-Name", "?")
            from_cache = resp.headers.get("X-From-Cache", "?") == "1"
    except Exception as e:
        return {"ts": t0, "uid": uid, "err": str(e), "class": "error"}

    has_avatar = '"avatar_url"' in text
    if ver == "v1":
        cls = "v1"
    elif ver == "v2" and has_avatar:
        cls = "fresh-v2"
    elif ver == "v2" and not has_avatar:
        cls = "stale-v2"
    else:
        cls = f"unknown-{ver}-{has_avatar}"
    return {"ts": t0, "uid": uid, "ver": ver, "pod": pod,
            "from_cache": from_cache, "class": cls}


def zipf(alpha, n):
    while True:
        # Inverse-CDF sample
        u = random.random()
        # rejection sampler — good enough for n=1000, alpha=0.9
        x = int(n * u ** (1.0 / (1.0 - alpha)))
        if 1 <= x <= n:
            return x


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--rps", type=float, default=80)
    ap.add_argument("--zipf-alpha", type=float, default=0.9)
    ap.add_argument("--zipf-n", type=int, default=1000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="probe")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    interval = 1.0 / args.rps
    t_end = time.time() + args.duration
    rows = []

    timeout = aiohttp.ClientTimeout(total=2)
    connector = aiohttp.TCPConnector(limit=200)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as s:
        # Tasks pool so requests don't queue if backend hiccups.
        async def driver():
            while time.time() < t_end:
                uid = zipf(args.zipf_alpha, args.zipf_n)
                row = await one_request(s, f"{args.url}/timeline/{uid}", uid)
                rows.append(row)
                # NB: simple sleep — under-target rate is fine here.
                await asyncio.sleep(interval)

        # Spawn a few drivers to actually approach the target rate.
        n_drivers = max(1, int(args.rps / 20))
        await asyncio.gather(*[driver() for _ in range(n_drivers)])

    # Write CSV-ish JSON-lines + summary.
    log_path = out_path / f"{args.label}.jsonl"
    with log_path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    cls_counts: dict[str, int] = {}
    for r in rows:
        cls_counts[r["class"]] = cls_counts.get(r["class"], 0) + 1

    summary = {
        "label": args.label,
        "url": args.url,
        "duration_s": args.duration,
        "target_rps": args.rps,
        "actual_total": len(rows),
        "actual_rps": len(rows) / args.duration,
        "class_counts": cls_counts,
        "stale_v2_count": cls_counts.get("stale-v2", 0),
        "fresh_v2_count": cls_counts.get("fresh-v2", 0),
        "v1_count": cls_counts.get("v1", 0),
        "error_count": cls_counts.get("error", 0),
    }
    v2_total = summary["stale_v2_count"] + summary["fresh_v2_count"]
    summary["stale_v2_rate"] = summary["stale_v2_count"] / v2_total if v2_total else 0
    (out_path / f"{args.label}.summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
