"""
analyze_logs.py — Post-run log analysis for wiki_rt_monitor (RTS2601).

Usage:
    python scripts/analyze_logs.py logs/

Reads:
    logs/overflow_events.csv   — backpressure drop-oldest events
    logs/deadline_misses.csv   — packets that exceeded the 2 ms deadline

Prints:
    1. Overflow summary: total drops by priority
    2. Deadline-miss summary: miss count and p50/p90/p99 latency by priority
    3. Overflow rate per minute (using total_drops as a proxy counter)
"""

import csv
import os
import statistics
import sys


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int((p / 100.0) * (len(sorted_data) - 1))
    return sorted_data[min(idx, len(sorted_data) - 1)]


def analyze_overflows(path: str) -> None:
    if not os.path.exists(path):
        print(f"[overflow] {path} not found — no overflow events recorded.")
        return

    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        print("[overflow] File is empty — no overflow events.")
        return

    by_priority: dict[str, int] = {}
    for row in rows:
        pri = row.get("priority", "Unknown")
        by_priority[pri] = by_priority.get(pri, 0) + 1

    total = len(rows)
    print(f"\n{'─'*50}")
    print("OVERFLOW EVENTS SUMMARY")
    print(f"{'─'*50}")
    print(f"  Total overflow events:  {total}")
    for pri, count in sorted(by_priority.items()):
        pct = 100.0 * count / total if total else 0.0
        print(f"  {pri:8s}:  {count:6d}  ({pct:.1f}%)")

    # Estimate drops per minute using the total_drops counter in the last row.
    last_total = int(rows[-1].get("total_drops", 0))
    print(f"  Running drop total (last row): {last_total}")


def analyze_deadline_misses(path: str) -> None:
    if not os.path.exists(path):
        print(f"\n[deadline] {path} not found — no deadline misses recorded.")
        return

    by_priority: dict[str, list[float]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pri = row.get("priority", "Unknown")
            try:
                lat = float(row["latency_us"])
            except (KeyError, ValueError):
                continue
            by_priority.setdefault(pri, []).append(lat)

    all_lats = [l for lats in by_priority.values() for l in lats]

    print(f"\n{'─'*50}")
    print("DEADLINE MISS SUMMARY  (deadline = 2,000 µs)")
    print(f"{'─'*50}")

    if not all_lats:
        print("  No deadline misses recorded.")
        return

    print(f"  Total misses: {len(all_lats)}")
    print(f"  Overall p50/p90/p99 (µs): "
          f"{percentile(all_lats, 50):.1f} / "
          f"{percentile(all_lats, 90):.1f} / "
          f"{percentile(all_lats, 99):.1f}")
    print()
    print(f"  {'Priority':<10} {'Count':>6}  {'Mean µs':>10}  {'p50':>8}  {'p90':>8}  {'p99':>8}  {'Max':>8}")
    print(f"  {'-'*10} {'-'*6}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}")
    for pri, lats in sorted(by_priority.items()):
        mean = statistics.mean(lats) if lats else 0.0
        print(f"  {pri:<10} {len(lats):>6}  {mean:>10.1f}  "
              f"{percentile(lats, 50):>8.1f}  "
              f"{percentile(lats, 90):>8.1f}  "
              f"{percentile(lats, 99):>8.1f}  "
              f"{max(lats):>8.1f}")


def main() -> None:
    log_dir = sys.argv[1] if len(sys.argv) > 1 else "logs"
    if not os.path.isdir(log_dir):
        print(f"Error: '{log_dir}' is not a directory.")
        sys.exit(1)

    print(f"Analyzing logs in: {os.path.abspath(log_dir)}")
    analyze_overflows(os.path.join(log_dir, "overflow_events.csv"))
    analyze_deadline_misses(os.path.join(log_dir, "deadline_misses.csv"))
    print(f"\n{'─'*50}")
    print("Done.")


if __name__ == "__main__":
    main()
