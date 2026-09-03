#!/usr/bin/env python3
"""Summarize 32-node Stock vs WarmScale/SentryCache experiment outputs."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def trajectory_min_after(summary: dict, mark_ts: float | None) -> float:
    rows = summary.get("trajectory", [])
    if not rows:
        return 0.0
    if mark_ts is None:
        return min(float(r.get("hit_rate", 0.0)) for r in rows)
    return min(float(r.get("hit_rate", 0.0)) for r in rows if float(r.get("sec_unix", 0)) >= mark_ts)


def scaleout_rows(root: Path) -> list[dict]:
    out = []
    base = root / "scaleout"
    for summary_path in sorted(base.glob("*/*/observe.summary.json")):
        cond = summary_path.parts[-3]
        trial = summary_path.parts[-2].replace("trial_", "")
        summary = read_json(summary_path)
        warmup = read_json(summary_path.with_name("warmup.summary.json")) if summary_path.with_name("warmup.summary.json").exists() else {}
        out.append({
            "experiment": "scaleout",
            "condition": cond,
            "trial": trial,
            "overall_hit_rate": summary.get("overall_hit_rate", 0),
            "min_post_hit_rate": trajectory_min_after(summary, summary.get("mark_ts")),
            "errors": summary.get("errors", 0),
            "actual_rps": summary.get("actual_rps", 0),
            "warmup_hit_rate": warmup.get("overall_hit_rate", 0),
        })
    return out


def migration_rows(root: Path) -> list[dict]:
    out = []
    base = root / "migration"
    for summary_path in sorted(base.glob("*/*/*/observe.summary.json")):
        cond = summary_path.parts[-4]
        scenario = summary_path.parts[-3]
        trial = summary_path.parts[-2].replace("trial_", "")
        summary = read_json(summary_path)
        out.append({
            "experiment": f"migration_{scenario}",
            "condition": cond,
            "trial": trial,
            "overall_hit_rate": summary.get("overall_hit_rate", 0),
            "min_post_hit_rate": trajectory_min_after(summary, summary.get("mark_ts")),
            "errors": summary.get("errors", 0),
            "actual_rps": summary.get("actual_rps", 0),
            "warmup_hit_rate": "",
        })
    return out


def rolling_rows(root: Path) -> list[dict]:
    out = []
    base = root / "rolling"
    for summary_path in sorted(base.glob("*/*/observe.summary.json")):
        cond = summary_path.parts[-3]
        trial = summary_path.parts[-2].replace("trial_", "")
        summary = read_json(summary_path)
        stale_rate = float(summary.get("stale_v2_rate", 0.0))
        out.append({
            "experiment": "rolling",
            "condition": cond,
            "trial": trial,
            "overall_hit_rate": 1.0 - stale_rate,
            "min_post_hit_rate": stale_rate,
            "errors": summary.get("error_count", summary.get("errors", 0)),
            "actual_rps": summary.get("actual_rps", 0),
            "warmup_hit_rate": "",
        })
    return out


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["experiment", "condition", "trial", "overall_hit_rate", "min_post_hit_rate", "errors", "actual_rps", "warmup_hit_rate"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_grouped_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["experiment", "condition", "n", "mean_hit_rate", "mean_min_post_hit_rate", "errors"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def grouped(rows: list[dict]) -> list[dict]:
    keys = sorted({(r["experiment"], r["condition"]) for r in rows})
    out = []
    for experiment, condition in keys:
        subset = [r for r in rows if r["experiment"] == experiment and r["condition"] == condition]
        values = [float(r["overall_hit_rate"]) for r in subset if str(r["overall_hit_rate"]) != ""]
        mins = [float(r["min_post_hit_rate"]) for r in subset if str(r["min_post_hit_rate"]) != ""]
        out.append({
            "experiment": experiment,
            "condition": condition,
            "n": len(subset),
            "mean_hit_rate": statistics.mean(values) if values else 0.0,
            "mean_min_post_hit_rate": statistics.mean(mins) if mins else 0.0,
            "errors": sum(int(float(r["errors"])) for r in subset if str(r["errors"]) != ""),
        })
    return out


def write_svg(path: Path, rows: list[dict]) -> None:
    summary = grouped(rows)
    width = 1000
    height = max(260, 80 + 28 * len(summary))
    left = 260
    bar_max = 650
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="24" y="34" font-family="Arial" font-size="22" font-weight="700">32-node Stock vs WarmScale/SentryCache Summary</text>',
    ]
    for i, row in enumerate(summary):
        y = 70 + i * 28
        value = float(row["mean_hit_rate"])
        bar = max(1, int(value * bar_max))
        color = {"stock": "#6b7280", "sentrycache": "#0f766e", "np": "#ca8a04", "nm": "#dc2626", "nv": "#7c3aed"}.get(row["condition"], "#2563eb")
        label = f'{row["experiment"]}/{row["condition"]} n={row["n"]}'
        lines.append(f'<text x="24" y="{y + 16}" font-family="Arial" font-size="13">{label}</text>')
        lines.append(f'<rect x="{left}" y="{y}" width="{bar}" height="18" fill="{color}"/>')
        lines.append(f'<text x="{left + bar + 8}" y="{y + 14}" font-family="Arial" font-size="12">{value:.3f}</text>')
    lines.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True)
    args = parser.parse_args()

    root = Path(args.results)
    rows = scaleout_rows(root) + migration_rows(root) + rolling_rows(root)
    write_csv(root / "summary" / "warmscale_32node_trials.csv", rows)
    write_grouped_csv(root / "summary" / "warmscale_32node_grouped.csv", grouped(rows))
    write_svg(root / "figures" / "warmscale_32node_stock_vs_warmscale.svg", rows)
    print(json.dumps({"rows": len(rows), "results": str(root)}, indent=2))


if __name__ == "__main__":
    main()
