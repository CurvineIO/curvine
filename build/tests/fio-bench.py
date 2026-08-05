#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import time

DEFAULT_DIR = "/curvine-fuse/fio-bench"
DEFAULT_THREADS = 32
DEFAULT_FILE_SIZE = "1G"
DEFAULT_BLOCK_SIZE = "256KB"

TESTS = [
    ("write", "Sequential write", "Throughput"),
    ("read", "Sequential read", "Throughput"),
    ("randwrite", "Random write", "Random"),
    ("randread", "Random read", "Random"),
]

HEADER = ["ITEM", "SPEED(GiB/s)", "IOPS", "AVG COST", "P50(ms)", "P95(ms)", "P99(ms)", "MAX(ms)", "SAMPLES", "ERRORS"]


def parse_size(value):
    value = str(value).strip().upper()
    suffixes = {
        "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4,
        "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4,
    }
    for suffix, factor in suffixes.items():
        if value.endswith(suffix):
            return int(float(value[:-len(suffix)]) * factor)
    return int(float(value))


def parse_duration(value):
    value = str(value).strip().lower()
    if value.endswith("h"):
        return int(float(value[:-1]) * 3600)
    if value.endswith("m"):
        return int(float(value[:-1]) * 60)
    if value.endswith("s"):
        return int(float(value[:-1]))
    return int(float(value))


def format_bytes(value):
    units = ["B", "KB", "MB", "GB", "TB"]
    v = float(value)
    for unit in units:
        if v < 1024 or unit == "TB":
            return f"{v:.1f}{unit}"
        v /= 1024
    return f"{v:.1f}TB"


def ensure_dir(path):
    abs_path = os.path.abspath(path)
    if os.path.exists(abs_path):
        home = os.path.expanduser("~")
        cwd = os.getcwd()
        if abs_path == os.path.sep or abs_path == home or abs_path == cwd or os.path.dirname(abs_path) == abs_path:
            sys.exit(f"Refuse to remove unsafe directory: {abs_path}")
        print(f"Removing existing directory: {abs_path}")
        shutil.rmtree(abs_path)
    os.makedirs(abs_path, exist_ok=True)
    return abs_path


def first_data_file_size(directory):
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            return os.path.getsize(path)
    return None


def run_fio(rw, directory, threads, size, block_size, duration, ioengine, direct):
    cmd = [
        "fio",
        "--name=fio_data",
        "--directory=" + directory,
        "--rw=" + rw,
        "--bs=" + str(block_size),
        "--size=" + str(size),
        "--numjobs=" + str(threads),
        "--ioengine=" + ioengine,
        "--direct=" + str(direct),
        "--group_reporting",
        "--output-format=json",
    ]
    if duration:
        cmd += ["--runtime=" + str(duration), "--time_based=1"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"fio failed for rw={rw}: {detail}")
    return json.loads(result.stdout)


def extract_metrics(data, rw, block_size):
    job = data["jobs"][0]
    section = "write" if rw in ("write", "randwrite") else "read"
    metric = job.get(section, {})
    if not metric:
        return None
    clat = metric.get("clat_ns", {})
    percentile = clat.get("percentile", {})
    return {
        "rw": rw,
        "io_bytes": metric.get("io_bytes", 0),
        "iops": metric.get("iops", 0),
        "bw_bytes": metric.get("bw_bytes", 0),
        "avg_cost_ms": clat.get("mean", 0) / 1e6,
        "p50_ms": percentile.get("50.000000", 0) / 1e6,
        "p95_ms": percentile.get("95.000000", 0) / 1e6,
        "p99_ms": percentile.get("99.000000", 0) / 1e6,
        "max_ms": clat.get("max", 0) / 1e6,
        "samples": metric.get("io_bytes", 0) // block_size if block_size else 0,
        "errors": job.get("error", 0),
    }


def speed_string(metric):
    return f"{metric['bw_bytes'] / (1024 ** 3):.2f}"


def iops_string(metric):
    return f"{metric['iops']:.2f}"


def progress_string(metric):
    return f"{metric['bw_bytes'] / (1024 ** 3):.2f} GiB/s, {metric['iops']:.2f} IOPS, p99={metric['p99_ms']:.2f}ms"


def build_report(metrics):
    rows = []
    for (rw, item, _), metric in zip(TESTS, metrics):
        rows.append([
            item,
            speed_string(metric),
            iops_string(metric),
            f"{metric['avg_cost_ms']:.2f} ms/op",
            f"{metric['p50_ms']:.2f}",
            f"{metric['p95_ms']:.2f}",
            f"{metric['p99_ms']:.2f}",
            f"{metric['max_ms']:.2f}",
            str(metric["samples"]),
            str(metric["errors"]),
        ])
    return [("Fio Benchmark", rows)]


def print_table(title, rows):
    print()
    print(title + ":")
    if not rows:
        print("(no results)")
        return
    widths = []
    for index in range(len(HEADER)):
        max_len = len(HEADER[index])
        for row in rows:
            max_len = max(max_len, len(row[index]))
        widths.append(max_len)
    header_line = "| " + " | ".join(HEADER[i].ljust(widths[i]) for i in range(len(HEADER))) + " |"
    separator = "| " + " | ".join("-" * widths[i] for i in range(len(HEADER))) + " |"
    print(header_line)
    print(separator)
    for row in rows:
        cells = [row[0].ljust(widths[0])]
        cells += [row[i].rjust(widths[i]) for i in range(1, len(row))]
        print("| " + " | ".join(cells) + " |")


def json_report(metrics):
    items = []
    for (rw, item, _), metric in zip(TESTS, metrics):
        items.append({
            "item": item,
            "rw": rw,
            "speed_gib_s": metric["bw_bytes"] / (1024 ** 3),
            "iops": metric["iops"],
            "avg_cost_ms": metric["avg_cost_ms"],
            "p50_ms": metric["p50_ms"],
            "p95_ms": metric["p95_ms"],
            "p99_ms": metric["p99_ms"],
            "max_ms": metric["max_ms"],
            "samples": metric["samples"],
            "errors": metric["errors"],
        })
    return json.dumps({"tests": items}, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Run fio sequential/random read-write benchmarks and print a curvine-cli bench style report.")
    parser.add_argument("--directory", default=DEFAULT_DIR, help=f"Benchmark directory, created or recreated (default: {DEFAULT_DIR})")
    parser.add_argument("-p", "--threads", type=int, default=DEFAULT_THREADS, help=f"Concurrency, number of fio jobs (default: {DEFAULT_THREADS})")
    parser.add_argument("--file-size", default=DEFAULT_FILE_SIZE, help=f"File size per job (default: {DEFAULT_FILE_SIZE})")
    parser.add_argument("-b", "--block-size", default=DEFAULT_BLOCK_SIZE, help=f"Block size (default: {DEFAULT_BLOCK_SIZE})")
    parser.add_argument("--duration", default=None, help="Run duration per test, e.g. 30s/1m (default: run to completion)")
    parser.add_argument("--ioengine", default="libaio", help="fio ioengine (default: libaio)")
    parser.add_argument("--direct", type=int, default=1, help="O_DIRECT flag, 0 or 1 (default: 1)")
    parser.add_argument("--json", action="store_true", help="Print report as JSON")
    args = parser.parse_args()

    if shutil.which("fio") is None:
        sys.exit("fio not found in PATH, please install fio first")
    if args.threads <= 0:
        sys.exit("--threads must be greater than 0")
    block_size = parse_size(args.block_size)
    if block_size <= 0:
        sys.exit("--block-size must be greater than 0")
    file_size = parse_size(args.file_size)
    if file_size <= 0:
        sys.exit("--file-size must be greater than 0")
    duration = parse_duration(args.duration) if args.duration else None

    directory = ensure_dir(args.directory)
    print()
    print("Configuration: fio")
    print(
        f"Target: Fuse, Path: {args.directory}, Threads: {args.threads}, "
        f"FileSize: {args.file_size}, BlockSize: {args.block_size}, "
        f"Duration: {args.duration or 'none'}, IoEngine: {args.ioengine}, Direct: {args.direct}"
    )
    print(f"Estimated total data per test: {format_bytes(file_size * args.threads)}")
    print("Note: tests share one file set (write -> read -> randwrite -> randread)")

    metrics = []
    for index, (rw, item, _) in enumerate(TESTS, start=1):
        print(f"\n[{index}/{len(TESTS)}] {item}: running ...")
        start = time.time()
        if rw in ("read", "randread"):
            actual = first_data_file_size(directory)
            if actual is not None and actual > 0:
                size = actual
            else:
                size = file_size
        else:
            size = file_size
        data = run_fio(rw, directory, args.threads, size, block_size, duration, args.ioengine, args.direct)
        metric = extract_metrics(data, rw, block_size)
        if metric is None:
            sys.exit(f"no {rw} metrics in fio output")
        metrics.append(metric)
        print(f"    done in {time.time() - start:.1f}s, {progress_string(metric)}")

    print("\nBenchmark finished!")
    print(f"Temp path: {args.directory} (removed)")
    if args.json:
        print(json_report(metrics))
    else:
        for title, rows in build_report(metrics):
            print_table(title, rows)
    print(f"Removing benchmark directory: {directory}")
    shutil.rmtree(directory)


if __name__ == "__main__":
    main()
