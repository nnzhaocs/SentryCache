#!/usr/bin/env python3
"""Recompute the aggregated stats in figure_data.py from raw per-trial CSVs.

Verifies that the numbers hard-coded in figure_data.py (means, stds) match
what's in raw_data/*.csv. Run this if you change the underlying CSVs or
want to sanity-check the data flow.

    cd paper_data/figures_src
    python3 derive_stats.py
"""
from pathlib import Path
import csv
import statistics
from collections import defaultdict

RAW = Path(__file__).parent / "raw_data"


def load_csv(name):
    rows = []
    with (RAW / name).open() as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def stats(values):
    if not values:
        return float("nan"), float("nan")
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def fmt(name, mean, std, unit="", n=None):
    n_str = f" (n={n})" if n is not None else ""
    return f"  {name}{n_str}: {mean:.2f} ± {std:.2f}{unit}"


def main():
    print("=" * 70)
    print("§5.2 Cold-Start (Redis cache-svc)")
    print("=" * 70)
    rows = load_csv("fig_5_2_twarmup.csv")
    grouped = defaultdict(lambda: {"twarmup": [], "drop_pp": []})
    for r in rows:
        grouped[r["condition"]]["twarmup"].append(float(r["T_warmup_s"]))
        grouped[r["condition"]]["drop_pp"].append(float(r["cluster_drop_pp"]))
    for cond in ("stock", "sentrycache", "np"):
        g = grouped[cond]
        m_t, s_t = stats(g["twarmup"])
        m_d, s_d = stats(g["drop_pp"])
        print(f"[{cond}]")
        print(fmt("T_warmup", m_t, s_t, " s", n=len(g["twarmup"])))
        print(fmt("cluster_drop_pp", m_d, s_d, " pp", n=len(g["drop_pp"])))

    print()
    print("=" * 70)
    print("§5.2 Cold-Start (memcached cache-svc, apples-to-apples 8MB)")
    print("=" * 70)
    rows = load_csv("fig_5_2_memcached.csv")
    grouped = defaultdict(lambda: {"twarmup": [], "drop_pp": []})
    for r in rows:
        grouped[r["condition"]]["twarmup"].append(float(r["T_warmup_s"]))
        grouped[r["condition"]]["drop_pp"].append(float(r["cluster_drop_pp"]))
    for cond in ("stock", "sentrycache", "np"):
        g = grouped[cond]
        m_t, s_t = stats(g["twarmup"])
        m_d, s_d = stats(g["drop_pp"])
        print(f"[{cond}]")
        print(fmt("T_warmup", m_t, s_t, " s", n=len(g["twarmup"])))
        print(fmt("cluster_drop_pp", m_d, s_d, " pp", n=len(g["drop_pp"])))

    print()
    print("=" * 70)
    print("§5.2 ChainApp depth sweep (depths 1, 3 measured)")
    print("=" * 70)
    rows = load_csv("fig_5_2_chainapp.csv")
    grouped = defaultdict(lambda: defaultdict(list))
    for r in rows:
        grouped[(int(r["depth"]), r["condition"])]["twarmup"].append(float(r["T_warmup_s"]))
        grouped[(int(r["depth"]), r["condition"])]["drop"].append(float(r["cluster_drop_pp"]))
    for depth in (1, 3):
        print(f"\n--- depth {depth} ---")
        for cond in ("stock", "sentrycache"):
            g = grouped[(depth, cond)]
            m_t, s_t = stats(g["twarmup"])
            m_d, s_d = stats(g["drop"])
            print(f"[{cond}]")
            print(fmt("T_warmup", m_t, s_t, " s", n=len(g["twarmup"])))
            print(fmt("drop_pp", m_d, s_d, " pp", n=len(g["drop"])))

    print()
    print("=" * 70)
    print("§5.2 SocialNetwork (DSB, wrk2 aggregate latency)")
    print("=" * 70)
    rows = load_csv("fig_5_2_socialnetwork.csv")
    grouped = defaultdict(lambda: {"p50": [], "p99": []})
    for r in rows:
        grouped[r["condition"]]["p50"].append(float(r["p50_ms"]))
        grouped[r["condition"]]["p99"].append(float(r["p99_ms"]))
    for cond in ("stock", "sentrycache", "np"):
        g = grouped[cond]
        m50, s50 = stats(g["p50"])
        m99, s99 = stats(g["p99"])
        print(f"[{cond}]")
        print(fmt("p50", m50, s50, " ms", n=len(g["p50"])))
        print(fmt("p99", m99, s99, " ms", n=len(g["p99"])))

    print()
    print("=" * 70)
    print("§5.3 Version Isolation")
    print("=" * 70)
    rows = load_csv("fig_5_3_stale_rate.csv")
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["condition"]].append(float(r["stale_rate"]) * 100)
    for cond in ("stock", "sentrycache", "nv"):
        m, s = stats(grouped[cond])
        print(f"[{cond}]")
        print(fmt("stale_rate", m, s, " %", n=len(grouped[cond])))

    print()
    print("=" * 70)
    print("§5.4 Migration on Departure")
    print("=" * 70)
    rows = load_csv("fig_5_4_drop_recovery.csv")
    grouped = defaultdict(lambda: defaultdict(lambda: {"drop": [], "rec": []}))
    for r in rows:
        cond, scen = r["condition"], r["scenario"]
        grouped[scen][cond]["drop"].append(float(r["drop_pp"]))
        if r["recovery_s"] not in ("", "nan"):
            grouped[scen][cond]["rec"].append(float(r["recovery_s"]))
    for scen in ("scalein", "crash"):
        print(f"\n--- {scen} ---")
        for cond in ("stock", "sentrycache", "nm"):
            g = grouped[scen][cond]
            m_d, s_d = stats(g["drop"])
            m_r, s_r = stats(g["rec"])
            print(f"[{cond}]")
            print(fmt("drop_pp", m_d, s_d, " pp", n=len(g["drop"])))
            print(fmt("recovery_s", m_r, s_r, " s", n=len(g["rec"])))


if __name__ == "__main__":
    main()
