#!/usr/bin/env python3
"""Generate paper-oriented SVG figures for the 32-node WarmScale results."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


COLORS = {
    "stock": "#6b7280",
    "sentrycache": "#0f766e",
    "np": "#ca8a04",
    "nm": "#dc2626",
    "nv": "#7c3aed",
}

LABELS = {
    "stock": "Stock",
    "sentrycache": "WarmScale",
    "np": "No-prefetch",
    "nm": "No-migration",
    "nv": "No-version",
}


def esc(text: object) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def read_grouped(results: Path) -> dict[tuple[str, str], dict]:
    grouped_path = results / "summary" / "warmscale_32node_grouped.csv"
    with grouped_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return {(r["experiment"], r["condition"]): r for r in rows}


def metric(row: dict, field: str) -> float:
    return float(row[field])


def draw_axis(lines: list[str], x: int, y: int, width: int, ticks: tuple[float, ...]) -> None:
    lines.append(f'<line x1="{x}" y1="{y}" x2="{x + width}" y2="{y}" stroke="#d1d5db" stroke-width="1"/>')
    for tick in ticks:
        tx = x + tick * width
        lines.append(f'<line x1="{tx:.1f}" y1="{y - 4}" x2="{tx:.1f}" y2="{y + 4}" stroke="#d1d5db" stroke-width="1"/>')
        lines.append(f'<text x="{tx:.1f}" y="{y + 20}" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="11" fill="#4b5563">{tick:.1f}</text>')


def add_bar(
    lines: list[str],
    x: int,
    y: int,
    width: int,
    label: str,
    value: float,
    color: str,
    suffix: str = "",
) -> None:
    bar_width = max(1, value * width)
    lines.append(f'<text x="{x - 12}" y="{y + 14}" text-anchor="end" font-family="Arial, Helvetica, sans-serif" font-size="12" fill="#111827">{esc(label)}</text>')
    lines.append(f'<rect x="{x}" y="{y}" width="{bar_width:.1f}" height="16" rx="2" fill="{color}"/>')
    lines.append(f'<text x="{x + bar_width + 8:.1f}" y="{y + 13}" font-family="Arial, Helvetica, sans-serif" font-size="11" fill="#111827">{value:.3f}{esc(suffix)}</text>')


def figure_svg(
    *,
    output: Path,
    title: str,
    subtitle: str,
    experiment: str,
    conditions: list[str],
    rows: dict[tuple[str, str], dict],
    primary_label: str,
    primary_field: str,
    secondary_label: str,
    secondary_field: str,
    note: str,
) -> None:
    width = 980
    height = 340 + 76 * len(conditions)
    left = 250
    bar_width = 560
    y0 = 124
    row_gap = 76
    primary_y = 0
    secondary_y = 24
    axis_y = height - 62
    lines: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="34" y="42" font-family="Arial, Helvetica, sans-serif" font-size="24" font-weight="700" fill="#111827">{esc(title)}</text>',
        f'<text x="34" y="70" font-family="Arial, Helvetica, sans-serif" font-size="13" fill="#4b5563">{esc(subtitle)}</text>',
        f'<text x="{left}" y="103" font-family="Arial, Helvetica, sans-serif" font-size="12" font-weight="700" fill="#111827">{esc(primary_label)}</text>',
        f'<text x="{left + 280}" y="103" font-family="Arial, Helvetica, sans-serif" font-size="12" font-weight="700" fill="#374151">{esc(secondary_label)}</text>',
    ]

    for idx, cond in enumerate(conditions):
        row = rows[(experiment, cond)]
        base_y = y0 + idx * row_gap
        label = LABELS.get(cond, cond)
        color = COLORS.get(cond, "#2563eb")
        primary = metric(row, primary_field)
        secondary = metric(row, secondary_field)
        lines.append(f'<line x1="34" y1="{base_y - 18}" x2="{width - 34}" y2="{base_y - 18}" stroke="#f3f4f6" stroke-width="1"/>')
        add_bar(lines, left, base_y + primary_y, bar_width, label, primary, color)
        add_bar(lines, left, base_y + secondary_y, bar_width, "", secondary, "#d1d5db")
        lines.append(f'<text x="{left + bar_width + 86}" y="{base_y + 32}" font-family="Arial, Helvetica, sans-serif" font-size="11" fill="#4b5563">n={esc(row["n"])}; errors={esc(row["errors"])}</text>')

    draw_axis(lines, left, axis_y, bar_width, (0.0, 0.25, 0.5, 0.75, 1.0))
    lines.append(f'<text x="{left + bar_width / 2:.1f}" y="{axis_y + 42}" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="12" fill="#4b5563">rate</text>')
    lines.append(f'<text x="34" y="{height - 28}" font-family="Arial, Helvetica, sans-serif" font-size="12" fill="#4b5563">{esc(note)}</text>')
    lines.append("</svg>")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True, help="32-node result root")
    args = parser.parse_args()

    results = Path(args.results)
    rows = read_grouped(results)
    figures = results / "figures"

    figure_svg(
        output=figures / "fig_32node_scaleout.svg",
        title="Scale-out cache behavior on 32 nodes",
        subtitle="Stock, WarmScale, and no-prefetch ablation; three trials per condition.",
        experiment="scaleout",
        conditions=["stock", "sentrycache", "np"],
        rows=rows,
        primary_label="Mean observe hit rate",
        primary_field="mean_hit_rate",
        secondary_label="Post-scale trough hit rate",
        secondary_field="mean_min_post_hit_rate",
        note="Conclusion: WarmScale runs end-to-end and gives a small post-event trough improvement; this is a conservative scale-out claim.",
    )

    figure_svg(
        output=figures / "fig_32node_migration_scalein.svg",
        title="Scale-in migration on 32 nodes",
        subtitle="Stock, WarmScale, and no-migration ablation; three trials per condition.",
        experiment="migration_scalein",
        conditions=["stock", "sentrycache", "nm"],
        rows=rows,
        primary_label="Mean observe hit rate",
        primary_field="mean_hit_rate",
        secondary_label="Post-event trough hit rate",
        secondary_field="mean_min_post_hit_rate",
        note="Conclusion: WarmScale improves the scale-in trough relative to Stock and no-migration, but the error count must be reported.",
    )

    figure_svg(
        output=figures / "fig_32node_migration_crash.svg",
        title="Crash boundary under cache migration",
        subtitle="Stock, WarmScale, and no-migration ablation; three trials per condition.",
        experiment="migration_crash",
        conditions=["stock", "sentrycache", "nm"],
        rows=rows,
        primary_label="Mean observe hit rate",
        primary_field="mean_hit_rate",
        secondary_label="Post-crash trough hit rate",
        secondary_field="mean_min_post_hit_rate",
        note="Conclusion: crash handling should be framed as robustness/boundary evidence, not as the main WarmScale advantage.",
    )

    figure_svg(
        output=figures / "fig_32node_rolling_version.svg",
        title="Version isolation during rolling update",
        subtitle="Stock, WarmScale, and no-version ablation; three trials per condition.",
        experiment="rolling",
        conditions=["stock", "sentrycache", "nv"],
        rows=rows,
        primary_label="Fresh-v2 response rate",
        primary_field="mean_hit_rate",
        secondary_label="Stale-v2 response rate",
        secondary_field="mean_min_post_hit_rate",
        note="Conclusion: WarmScale eliminates stale-v2 responses in this 32-node rolling update experiment.",
    )

    print(f"Generated paper figures under {figures}")


if __name__ == "__main__":
    main()
