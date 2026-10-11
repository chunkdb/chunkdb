#!/usr/bin/env python3
"""Compare two builds with self-contained hot-path budget tables.

Alternate builds for 15 runs per profile by default. The relaxed profile gates
median requests/s at a 5% drop; fsync-wal is reported only. Passing the same
build twice measures the noise floor. Rebuild both benchmarks with block-count
and build-provenance reporting before comparing them.

    scripts/bench/compare_budgets.py BEFORE AFTER --markdown --json result.json
"""

import argparse
import hashlib
import json
import math
import os
import platform
import plistlib
import socket
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# name, durability mode, requests, clients, keyspace, gates
PROFILES = [
    ("relaxed", "relaxed", 200000, 16, 2048, True),
    ("fsync-wal", "fsync-wal", 5000, 16, 256, False),
]
SCENARIOS = "world,canvas,simulation"
KINDS = ("ping", "set_block", "get_block", "set_chunk", "get_chunk", "get_area")
DESCRIPTIONS = {
    "world": "Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK.",
    "canvas": "Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK.",
    "simulation": "Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET.",
}
BUILD_FIELDS = ("source_commit", "source_state", "build_type", "benchmark_source_sha256",
                "chunk_width_blocks", "chunk_height_blocks")


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command_output(command):
    """Optional machine queries are marked unavailable, never guessed."""
    try:
        return subprocess.run(command, check=True, capture_output=True, timeout=10).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def text_query(command):
    value = command_output(command)
    return value.decode().strip() if value else "unavailable"


def filesystem_info(directory):
    result = {"path": os.path.realpath(directory), "type": "unavailable"}
    if sys.platform == "darwin":
        output = command_output(["df", "-P", directory])
        if output:
            row = output.decode().splitlines()[-1].split(None, 5)
            if len(row) == 6:
                result["mount"] = row[-1]
                info = command_output(["diskutil", "info", "-plist", row[-1]])
                if info:
                    data = plistlib.loads(info)
                    result["type"] = data.get("FilesystemType", data.get("FilesystemName", "unavailable"))
    elif sys.platform.startswith("linux"):
        result["type"] = text_query(["stat", "-f", "-c", "%T", directory])
    elif os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.windll.kernel32
        kernel.GetVolumePathNameW.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
        kernel.GetVolumeInformationW.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD,
                                               ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD),
                                               ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR, wintypes.DWORD)
        volume = ctypes.create_unicode_buffer(1024)
        if kernel.GetVolumePathNameW(str(Path(directory)), volume, len(volume)):
            name = ctypes.create_unicode_buffer(256)
            if kernel.GetVolumeInformationW(volume.value, None, 0, None, None, None, name, len(name)):
                result.update(type=name.value, mount=volume.value)
    return result


def machine_info():
    data = {"model": "unavailable", "cpu": "unavailable", "logical_cores": os.cpu_count(),
            "physical_cores": None, "ram_bytes": None, "architecture": platform.machine(),
            "os": platform.system(), "os_version": platform.platform(),
            "filesystem": filesystem_info(tempfile.gettempdir()),
            "load_sampling": "start and end of each benchmark process; observed ranges, not continuous sampling"}
    if sys.platform == "darwin":
        data["model"] = text_query(["sysctl", "-n", "hw.model"])
        data["cpu"] = text_query(["sysctl", "-n", "machdep.cpu.brand_string"])
        for key, field in (("hw.physicalcpu", "physical_cores"), ("hw.memsize", "ram_bytes")):
            value = text_query(["sysctl", "-n", key])
            data[field] = int(value) if value.isdigit() else None
        data["os_version"] = "macOS " + platform.mac_ver()[0] + " (" + platform.release() + ")"
    elif sys.platform.startswith("linux"):
        model = Path("/sys/devices/virtual/dmi/id/product_name")
        if model.is_file():
            data["model"] = model.read_text().strip()
        cpu = Path("/proc/cpuinfo")
        if cpu.is_file():
            rows = [row.split(":", 1)[1].strip() for row in cpu.read_text().splitlines() if row.startswith("model name")]
            if rows:
                data["cpu"] = rows[0]
        memory = Path("/proc/meminfo")
        if memory.is_file():
            for row in memory.read_text().splitlines():
                if row.startswith("MemTotal:"):
                    data["ram_bytes"] = int(row.split()[1]) * 1024
        topology = {path.read_text().strip() + ":" + path.with_name("physical_package_id").read_text().strip()
                    for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/topology/core_id")
                    if path.with_name("physical_package_id").is_file()}
        data["physical_cores"] = len(topology) or None
    elif os.name == "nt":
        # Built-in OS queries only; absence remains visible in the report.
        script = "$c=Get-CimInstance Win32_ComputerSystem; $p=@(Get-CimInstance Win32_Processor); " \
                 "@{model=$c.Model;cpu=($p.Name -join '; ');ram_bytes=$c.TotalPhysicalMemory;" \
                 "physical_cores=($p.NumberOfCores | Measure-Object -Sum).Sum} | ConvertTo-Json -Compress"
        output = command_output(["powershell.exe", "-NoProfile", "-Command", script])
        if output:
            data.update(json.loads(output))
    if hasattr(os, "getpriority"):
        data["process_niceness"] = os.getpriority(os.PRIO_PROCESS, 0)
    return data


def load_average():
    return list(os.getloadavg()) if hasattr(os, "getloadavg") else None


def file_hash(filename):
    with open(filename, "rb") as stream:
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
        return digest.hexdigest()


def positive_number(value, name, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"Invalid {name}: {value!r}")


IO_TIMEOUT_MS = 5000


def validate_run(report, mode, requests, clients, keyspace, seed):
    for field, expected in (("server_mode", "spawn"), ("requests", requests), ("requested_clients", clients),
                            ("pipeline", 1), ("io_timeout_ms", IO_TIMEOUT_MS), ("keyspace", keyspace), ("seed", seed), ("durability_mode", mode)):
        if report.get(field) != expected:
            raise ValueError(f"Benchmark {field}: expected {expected!r}, got {report.get(field)!r}")
    for field in BUILD_FIELDS:
        if field not in report:
            raise ValueError(f"Benchmark lacks {field}; rebuild both benchmarks with reporting support")
    fingerprint = report["benchmark_source_sha256"]
    if not isinstance(fingerprint, str) or len(fingerprint) != 64 or any(char not in "0123456789abcdef" for char in fingerprint):
        raise ValueError("Invalid benchmark source SHA256")
    if not report["source_commit"] or not report["build_type"]:
        raise ValueError("Empty benchmark source/build provenance")
    width, height = report["chunk_width_blocks"], report["chunk_height_blocks"]
    if type(width) is not int or type(height) is not int or width <= 0 or height <= 0:
        raise ValueError("Invalid benchmark chunk geometry")
    rows = report.get("results", [])
    if len(rows) != len(SCENARIOS.split(",")) or {r["test"] for r in rows} != set(SCENARIOS.split(",")):
        raise ValueError("Benchmark scenario set is incomplete or duplicated")
    for row in rows:
        if row.get("completed_requests") != requests:
            raise ValueError("Benchmark request count mismatch")
        mix = row.get("request_mix")
        if not isinstance(mix, dict) or set(mix) != set(KINDS) or any(type(count) is not int or count < 0 for count in mix.values()) or sum(mix.values()) != requests:
            raise ValueError("Benchmark lacks valid actual request mix counts")
        blocks, area_chunks = row.get("completed_blocks"), row.get("returned_area_chunks")
        if type(blocks) is not int or blocks < 0 or type(area_chunks) is not int or area_chunks < 0:
            raise ValueError("Benchmark lacks valid completed block/area counts")
        expected_blocks = mix["set_block"] + mix["get_block"] + width * height * (mix["set_chunk"] + mix["get_chunk"] + area_chunks)
        if blocks != expected_blocks:
            raise ValueError("Benchmark block count disagrees with actual mix/geometry")
        for name in ("duration_s", "throughput_req_s", "blocks_per_request", "throughput_blocks_s"):
            positive_number(row.get(name), name, allow_zero=name != "duration_s" and name != "throughput_req_s")
        for name in ("p50", "p99"):
            positive_number(row.get("latency_ms", {}).get(name), name, allow_zero=True)
        if abs(row["blocks_per_request"] - blocks / requests) > 0.0000005001:
            raise ValueError("Benchmark blocks/request denominator mismatch")
        # The benchmark prints six decimal places. Account for duration rounding
        # when checking rates from a very short run, rather than a fixed tolerance.
        duration = row["duration_s"]
        rounding = 0.0000005
        for name, count, rate_rounding in (("throughput_req_s", requests, 0.0005), ("throughput_blocks_s", blocks, rounding)):
            rate = count / duration
            tolerance = count * rounding / (duration * max(duration - rounding, rounding)) + rate_rounding + 1e-9
            if abs(row[name] - rate) > tolerance:
                raise ValueError(f"Benchmark {name} denominator mismatch")
    return report


def run_once(build_dir, mode, requests, clients, keyspace, seed, watch=None):
    bench = os.path.abspath(os.path.join(build_dir, "chunkdb_server_bench" + (".exe" if os.name == "nt" else "")))
    command = [bench, "--server-mode", "spawn", "--port", str(free_port()), "--tests", SCENARIOS,
               "--requests", str(requests), "--clients", str(clients), "--keyspace", str(keyspace),
               "--seed", str(seed), "--durability-mode", mode, "--server-workers", str(clients),
               "--io-timeout-ms", str(IO_TIMEOUT_MS), "--log-level", "error", "--output", "json"]
    if watch:
        command += ["--watch", watch]
    binary_hash = file_hash(bench)
    before = load_average()
    output = subprocess.run(command, check=True, capture_output=True, text=True)
    after = load_average()
    if file_hash(bench) != binary_hash:
        raise ValueError("Benchmark binary changed during measurement")
    parsed = json.loads(output.stdout[output.stdout.index("{"):])
    validate_run(parsed, mode, requests, clients, keyspace, seed)
    return {"command": command, "binary_sha256": binary_hash, "load_before": before,
            "load_after": after, "stderr": output.stderr, "report": parsed}


def summary(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "runs": len(values)}


def summarize_side(samples, scenario):
    rows = [next(r for r in sample["report"]["results"] if r["test"] == scenario) for sample in samples]
    result = {name: summary([row[name] for row in rows]) for name in
              ("throughput_req_s", "throughput_blocks_s", "blocks_per_request", "completed_blocks", "returned_area_chunks")}
    for name in ("p50", "p99"):
        result[f"{name}_ms"] = summary([row["latency_ms"][name] for row in rows])
    result["request_mix_percent"] = {kind: summary([100 * row["request_mix"][kind] / row["completed_requests"] for row in rows]) for kind in KINDS}
    return result


def summarize_profile(name, mode, requests, clients, keyspace, gates, samples, budget, watch):
    builds = {}
    observations = []
    for side, runs in samples.items():
        first = runs[0]
        builds[side] = {field: first["report"][field] for field in BUILD_FIELDS}
        builds[side]["binary_sha256"] = first["binary_sha256"]
        builds[side]["directory"] = str(Path(first["command"][0]).parent)
        for sample in runs:
            if any(sample["report"][field] != builds[side][field] for field in BUILD_FIELDS) or sample["binary_sha256"] != builds[side]["binary_sha256"]:
                raise ValueError(f"{side} build/geometry changed between runs")
            observations.extend(value for value in (sample["load_before"], sample["load_after"]) if value is not None)
    if any(builds["before"][field] != builds["after"][field] for field in ("chunk_width_blocks", "chunk_height_blocks", "build_type")):
        raise ValueError("Compared builds must use the same geometry and build type")
    results = []
    for scenario in SCENARIOS.split(","):
        before, after = (summarize_side(samples[side], scenario) for side in ("before", "after"))
        change = 100 * (after["throughput_req_s"]["median"] / before["throughput_req_s"]["median"] - 1)
        verdict = "reported only" if not gates else "ok" if change >= -budget else "OVER BUDGET"
        results.append({"scenario": scenario, "request": DESCRIPTIONS[scenario], "before": before,
                        "after": after, "change_percent": change, "verdict": verdict})
    load = [{"min": min(row[i] for row in observations), "max": max(row[i] for row in observations)} for i in range(3)] if observations else None
    return {"name": name, "durability_mode": mode, "requests_per_run": requests, "clients": clients,
            "server_workers": clients, "pipeline": 1, "io_timeout_ms": IO_TIMEOUT_MS, "keyspace_blocks_per_axis": keyspace,
            "runs_per_build": len(samples["before"]), "watch_before": None, "watch_after": watch,
            "gates": gates, "budget_percent": budget, "builds": builds, "load_average_range": load,
            "results": results, "samples": samples,
            "passed": not gates or all(row["verdict"] == "ok" for row in results)}


def metric(value, digits=1):
    return f'{value["median"]:,.{digits}f} [{value["min"]:,.{digits}f}–{value["max"]:,.{digits}f}]'


def mix_text(side):
    return "; ".join(f"{kind} {metric(value, 3)}%" for kind, value in side["request_mix_percent"].items() if value["max"] > 0)


def render_markdown(report):
    machine = report["machine"]
    ram = f'{machine["ram_bytes"] / 1024**3:.2f} GiB' if machine["ram_bytes"] else "unavailable"
    lines = ["# Hot-path budget comparison", "", f'UTC: {report["started_utc"]} to {report["finished_utc"]}.', f'Machine: {machine["model"]}; CPU {machine["cpu"]}; '
             f'cores {machine["physical_cores"] or "unavailable"} physical / {machine["logical_cores"] or "unavailable"} logical; RAM {ram}.',
             f'OS: {machine["os_version"]}; architecture {machine["architecture"]}. '
             f'Benchmark temporary-store filesystem: {machine["filesystem"]["type"]} at `{machine["filesystem"]["path"]}`.',
             "Values are medians [min–max] across runs. Latency is the median of each run's p50/p99, with the range of those run percentiles.",
             "Blocks count positions handled by successful requests, not changed blocks or bytes; point requests count one and chunk/area forms use actual geometry and returned entries.",
             f'Load sampling: {machine["load_sampling"]}. Process niceness: {machine.get("process_niceness", "unavailable")}.', ""]
    for profile in report["profiles"]:
        load = " / ".join(f'{part["min"]:.2f}–{part["max"]:.2f}' for part in profile["load_average_range"]) if profile["load_average_range"] else "unavailable"
        lines += [f'## {profile["name"]}', "", f'Durability `{profile["durability_mode"]}`; clients/workers {profile["clients"]}/{profile["server_workers"]}; '
                  f'pipeline {profile["pipeline"]}; I/O timeout {profile["io_timeout_ms"]} ms; requests/run {profile["requests_per_run"]:,}; keyspace {profile["keyspace_blocks_per_axis"]} blocks/axis; '
                  f'{profile["runs_per_build"]} alternating runs/build; load average ranges (1/5/15 min) {load}.',
                  f'Gate: {profile["budget_percent"]}% req/s drop' if profile["gates"] else 'Gate: reported only (does not affect exit status).',
                  f'WATCH before: {profile["watch_before"] or "none"}; after: {profile["watch_after"] or "none"}.']
        for side, build in profile["builds"].items():
            lines += [f'{side.capitalize()}: `{build["source_commit"]}` ({build["source_state"]}), {build["build_type"]}; '
                      f'chunk {build["chunk_width_blocks"]}x{build["chunk_height_blocks"]}; binary SHA256 `{build["binary_sha256"]}`; '
                      f'benchmark source SHA256 `{build["benchmark_source_sha256"]}`.']
        lines += ["", "| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Verdict | Change req/s |",
                  "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        for row in profile["results"]:
            before, after = row["before"], row["after"]
            cells = [f'`{row["scenario"]}`: {row["request"]}', mix_text(before) + " / " + mix_text(after),
                     metric(before["blocks_per_request"], 3) + " / " + metric(after["blocks_per_request"], 3)]
            for name, digits in (("throughput_req_s", 1), ("throughput_blocks_s", 1), ("p50_ms", 3), ("p99_ms", 3)):
                cells += [metric(before[name], digits), metric(after[name], digits)]
            cells += [row["verdict"], f'{row["change_percent"]:+.2f}%']
            lines.append("| " + " | ".join(cell.replace("|", "\\|") for cell in cells) + " |")
        lines.append("")
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("--runs", type=int, default=15, help="alternating runs per build (default 15)")
    parser.add_argument("--budget", type=float, default=5.0, help="allowed median req/s drop (default 5%%)")
    parser.add_argument("--profiles", default=",".join(profile[0] for profile in PROFILES))
    parser.add_argument("--watch", help="attach a WATCH only to the after build (table name)")
    parser.add_argument("--json", nargs="?", const="-", metavar="FILE", help="write full artifact JSON; omit FILE for stdout")
    parser.add_argument("--markdown", nargs="?", const="-", metavar="FILE", help="write pasteable Markdown; omit FILE for stdout")
    args = parser.parse_args(argv)
    wanted = args.profiles.split(",")
    if args.runs < 1 or not math.isfinite(args.budget) or args.budget < 0:
        parser.error("runs must be positive and budget must be finite/nonnegative")
    if not wanted or len(set(wanted)) != len(wanted) or set(wanted) - {profile[0] for profile in PROFILES}:
        parser.error("profiles must be distinct known profile names")
    if args.json == "-" and args.markdown == "-":
        parser.error("JSON and Markdown cannot both use stdout; give one an output filename")
    if args.json and args.markdown and args.json != "-" and args.markdown != "-" and Path(args.json).resolve() == Path(args.markdown).resolve():
        parser.error("JSON and Markdown require different output filenames")
    report = {"schema_version": 1, "started_utc": datetime.now(timezone.utc).isoformat(),
              "machine": machine_info(), "profiles": []}
    for name, mode, requests, clients, keyspace, gates in PROFILES:
        if name not in wanted:
            continue
        samples = {"before": [], "after": []}
        for run in range(args.runs):
            order = ("before", "after") if run % 2 == 0 else ("after", "before")
            for side in order:
                print(f"{name}: run{run + 1}/{args.runs} {side} load_average={load_average()}", file=sys.stderr, flush=True)
                sample = run_once(args.before if side == "before" else args.after, mode, requests, clients,
                                  keyspace, 1337 + run, args.watch if side == "after" else None)
                sample.update(pair=run + 1, order=order.index(side) + 1)
                samples[side].append(sample)
        report["profiles"].append(summarize_profile(name, mode, requests, clients, keyspace, gates, samples, args.budget, args.watch))
    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    report["passed"] = all(profile["passed"] for profile in report["profiles"])
    outputs = []
    if args.json is not None:
        outputs.append((args.json, json.dumps(report, indent=2, allow_nan=False) + "\n"))
    if args.markdown is not None or args.json is None:
        outputs.append((args.markdown or "-", render_markdown(report)))
    for destination, text in outputs:
        if destination == "-":
            print(text, end="")
        else:
            Path(destination).write_text(text)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
