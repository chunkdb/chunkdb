#!/usr/bin/env python3
"""Compare two chunkdb builds on the hot-path budget scenarios.

Runs chunkdb_server_bench in spawn mode from each build, alternating which
build goes first, and compares the median throughput of every scenario. In a
gating profile, a scenario whose median throughput drops by more than the
budget fails the comparison (exit status 1); the durable profile is reported
only, because disk syncs vary more than the budget between identical builds.
See CONTRIBUTING.md, "Hot-path budgets".

    scripts/bench/compare_budgets.py BEFORE_BUILD_DIR AFTER_BUILD_DIR

Passing the same build twice measures the noise floor of the machine.
"""

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys

# Each profile: (name, durability mode, requests, clients, keyspace, gates).
# The durable profile is small, since every write waits for the disk and
# filling a region writes each of its chunks durably, and it does not gate:
# identical builds differ in it by up to 7% (median of 15 runs on macOS).
PROFILES = [
    ("relaxed", "relaxed", 200000, 16, 2048, True),
    ("fsync-wal", "fsync-wal", 5000, 16, 256, False),
]
SCENARIOS = "world,canvas,simulation"


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_once(build_dir, mode, requests, clients, keyspace, seed, watch=None):
    bench = os.path.join(build_dir, "chunkdb_server_bench")
    command = [
        bench, "--server-mode", "spawn", "--port", str(free_port()),
        "--tests", SCENARIOS, "--requests", str(requests), "--clients", str(clients),
        "--keyspace", str(keyspace), "--seed", str(seed),
        "--durability-mode", mode, "--server-workers", str(clients),
        "--log-level", "error", "--output", "json",
    ]
    if watch:
        command += ["--watch", watch]
    out = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    report = json.loads(out[out.index("{"):])
    return {r["test"]: (r["throughput_req_s"], r["latency_ms"]["p99"]) for r in report["results"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("--runs", type=int, default=15, help="alternating runs per build (default 15)")
    parser.add_argument("--budget", type=float, default=5.0, help="allowed median throughput drop in percent (default 5)")
    parser.add_argument("--profiles", default=",".join(p[0] for p in PROFILES), help="comma list of profiles to run")
    parser.add_argument("--watch", help="attach a watch only to the after build (table name)")
    args = parser.parse_args()

    wanted = set(args.profiles.split(","))
    failed = False
    for name, mode, requests, clients, keyspace, gates in PROFILES:
        if name not in wanted:
            continue
        samples = {"before": [], "after": []}
        for run in range(args.runs):
            order = ["before", "after"] if run % 2 == 0 else ["after", "before"]
            for side in order:
                build = args.before if side == "before" else args.after
                if hasattr(os, "getloadavg"):
                    print(f"{name}: run {run + 1}/{args.runs} {side} load_average=" +
                          "/".join(f"{n:.2f}" for n in os.getloadavg()), file=sys.stderr, flush=True)
                samples[side].append(run_once(build, mode, requests, clients, keyspace, 1337 + run, args.watch if side == "after" else None))
            print(f"{name}: run {run + 1}/{args.runs}", file=sys.stderr)

        print(f"\nprofile={name} durability_mode={mode} requests={requests} clients={clients} "
              f"keyspace={keyspace} runs={args.runs} watch_after={args.watch or 'none'}")
        print(f"{'scenario':<12} {'before req/s':>14} {'after req/s':>14} {'change':>8} "
              f"{'before p99 ms':>14} {'after p99 ms':>14}  verdict")
        for scenario in SCENARIOS.split(","):
            before_tp = statistics.median(s[scenario][0] for s in samples["before"])
            after_tp = statistics.median(s[scenario][0] for s in samples["after"])
            before_p99 = statistics.median(s[scenario][1] for s in samples["before"])
            after_p99 = statistics.median(s[scenario][1] for s in samples["after"])
            change = (after_tp / before_tp - 1.0) * 100.0
            if not gates:
                verdict = "reported only"
            else:
                verdict = "ok" if change >= -args.budget else "OVER BUDGET"
            failed = failed or (gates and verdict != "ok")
            print(f"{scenario:<12} {before_tp:>14.0f} {after_tp:>14.0f} {change:>+7.1f}% "
                  f"{before_p99:>14.3f} {after_p99:>14.3f}  {verdict}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
