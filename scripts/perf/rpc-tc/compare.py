#!/usr/bin/env python3
"""Compare recorded block-transfer benchmark runs; never substitute for hardware validation."""
import argparse
import json
import math
import re
import statistics
import sys


def read_run(path):
    with open(path, encoding="utf-8") as stream:
        run = json.load(stream)
    for key in ("bytes", "elapsed_seconds", "client_cpu_seconds", "server_cpu_seconds", "latency_ms"):
        if key not in run:
            raise ValueError(f"{path}: missing {key}")
    for key in ("bytes", "elapsed_seconds"):
        if isinstance(run[key], bool) or not isinstance(run[key], (int, float)) or not math.isfinite(run[key]) or run[key] <= 0:
            raise ValueError(f"{path}: {key} must be positive")
    for key in ("client_cpu_seconds", "server_cpu_seconds"):
        if isinstance(run[key], bool) or not isinstance(run[key], (int, float)) or not math.isfinite(run[key]) or run[key] < 0:
            raise ValueError(f"{path}: {key} must be nonnegative")
    samples = run["latency_ms"]
    if not isinstance(samples, list) or len(samples) < 100 or any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x <= 0 for x in samples):
        raise ValueError(f"{path}: latency_ms must contain at least 100 positive finite samples")
    workload = run.get("workload")
    fields = ("file_bytes", "chunk_bytes", "reads", "warmup_reads", "concurrency", "source_sha256", "short_circuit", "read_ahead", "rate", "delay_per_direction")
    if not isinstance(workload, dict) or any(key not in workload for key in fields):
        raise ValueError(f"{path}: missing workload settings")
    if workload["short_circuit"] is not False or workload["read_ahead"] is not False:
        raise ValueError(f"{path}: remote reads and disabled read-ahead are required")
    for key in ("file_bytes", "chunk_bytes", "reads", "warmup_reads", "concurrency"):
        value = workload[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < (0 if key == "warmup_reads" else 1):
            raise ValueError(f"{path}: invalid {key}")
    if workload["file_bytes"] % workload["chunk_bytes"] or workload["reads"] * workload["concurrency"] != len(samples) or run["bytes"] != workload["chunk_bytes"] * workload["reads"] * workload["concurrency"]:
        raise ValueError(f"{path}: workload, byte count and samples disagree")
    if any(not isinstance(workload[key], str) or not workload[key] for key in ("rate", "delay_per_direction")):
        raise ValueError(f"{path}: missing network shaping settings")
    if not isinstance(workload["source_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", workload["source_sha256"]):
        raise ValueError(f"{path}: unverified workload build; use build.sh")
    binaries = run.get("binaries")
    if not isinstance(binaries, dict) or any(not isinstance(binaries.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", binaries[key]) for key in ("server_sha256", "client_sha256")):
        raise ValueError(f"{path}: server and client binary hashes are required")
    if run.get("transport") not in ("orpc", "grpc") or not isinstance(run.get("revision"), str) or not run["revision"]:
        raise ValueError(f"{path}: transport and revision are required")
    if run["client_cpu_seconds"] + run["server_cpu_seconds"] <= 0:
        raise ValueError(f"{path}: total CPU must be positive; increase workload duration")
    samples = sorted(samples)
    return {"throughput": run["bytes"] / run["elapsed_seconds"],
            "p99": samples[math.ceil(len(samples) * 0.99) - 1],
            "cpu_per_gib": (run["client_cpu_seconds"] + run["server_cpu_seconds"]) / (run["bytes"] / (1024 ** 3)),
            "workload": workload, "transport": run["transport"], "revision": run["revision"], "binaries": binaries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    base_args = parser.add_mutually_exclusive_group(required=True)
    base_args.add_argument("--orpc", nargs="+", help="ORPC baseline run JSON files")
    base_args.add_argument("--baseline", nargs="+", help="Baseline checkpoint run JSON files")
    candidate_args = parser.add_mutually_exclusive_group(required=True)
    candidate_args.add_argument("--grpc", nargs="+", help="gRPC candidate run JSON files")
    candidate_args.add_argument("--candidate", nargs="+", help="Candidate checkpoint run JSON files")
    args = parser.parse_args()
    checkpoints = args.baseline is not None
    if checkpoints != (args.candidate is not None):
        parser.error("use either --orpc/--grpc or --baseline/--candidate")
    base_paths = args.baseline if checkpoints else args.orpc
    candidate_paths = args.candidate if checkpoints else args.grpc
    base_label, candidate_label = ("baseline", "candidate") if checkpoints else ("orpc", "grpc")
    if len(base_paths) != len(candidate_paths):
        parser.error("provide the same number of runs for both groups")
    try:
        baseline = [read_run(path) for path in base_paths]
        candidate = [read_run(path) for path in candidate_paths]
        if not checkpoints and (any(run["transport"] != "orpc" for run in baseline) or any(run["transport"] != "grpc" for run in candidate)):
            raise ValueError("transport labels do not match their argument groups")
        all_runs = baseline + candidate
        if any(run["workload"] != all_runs[0]["workload"] for run in all_runs):
            raise ValueError("workload settings differ across runs")
        if len({run["revision"] for run in baseline}) != 1 or len({run["revision"] for run in candidate}) != 1:
            raise ValueError("each transport group must use one revision")
        for group in (baseline, candidate):
            if any(run["binaries"] != group[0]["binaries"] for run in group):
                raise ValueError("each transport group must use the same server and client binaries")
        med = lambda runs, key: statistics.median(run[key] for run in runs)
        ratios = {key: med(candidate, key) / med(baseline, key) for key in ("throughput", "p99", "cpu_per_gib")}
    except (OSError, ValueError, TypeError, ZeroDivisionError, json.JSONDecodeError) as error:
        print(f"invalid benchmark data: {error}", file=sys.stderr)
        return 2
    passed = ratios["throughput"] >= 0.85 and ratios["p99"] <= 1.10 and ratios["cpu_per_gib"] <= 1.15
    print(json.dumps({f"{candidate_label}_over_{base_label}": ratios,
                      "median": {base_label: {key: med(baseline, key) for key in ratios},
                                 candidate_label: {key: med(candidate, key) for key in ratios}},
                      "revisions": {base_label: baseline[0]["revision"], candidate_label: candidate[0]["revision"]},
                      "runs_per_transport": len(baseline), "provisional_pass": passed,
                      "note": "Valid only for matched workloads; tc results do not establish the hardware gate."}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
