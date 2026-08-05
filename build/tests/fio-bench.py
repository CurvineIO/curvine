#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import time

DEFAULT_DIR = "/curvine-fuse/fio-bench"
DEFAULT_THREADS = 16
DEFAULT_BLOCK_SIZE = "64K,256K,1M"
AUTO_IO_PER_FILE = 65536
MAX_FILE_SIZE = 1024 ** 3

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


def parse_bs_list(value):
    sizes = []
    for item in str(value).split(","):
        item = item.strip()
        if item:
            sizes.append(parse_size(item))
    return sizes


def auto_file_size(block_size):
    return min(block_size * AUTO_IO_PER_FILE, MAX_FILE_SIZE)


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


def run_fio(rw, directory, threads, size, block_size, iodepth, timeout, ioengine, direct):
    cmd = [
        "fio",
        "--name=fio_data",
        "--directory=" + directory,
        "--rw=" + rw,
        "--bs=" + str(block_size),
        "--size=" + str(size),
        "--numjobs=" + str(threads),
        "--iodepth=" + str(iodepth),
        "--ioengine=" + ioengine,
        "--direct=" + str(direct),
        "--group_reporting",
        "--output-format=json",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout if timeout > 0 else None)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"fio timed out after {timeout}s for rw={rw} bs={block_size}")
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"fio failed for rw={rw} bs={block_size}: {detail}")
    return json.loads(result.stdout)


def extract_metrics(data, rw, block_size):
    jobs = data["jobs"]
    section = "write" if rw in ("write", "randwrite") else "read"
    total_io_bytes = 0
    total_iops = 0.0
    total_bw = 0
    total_errors = 0
    total_samples = 0
    weighted_mean = 0.0
    max_latency = 0.0
    p50 = p95 = p99 = 0.0
    found = False
    for job in jobs:
        metric = job.get(section, {})
        if not metric:
            continue
        found = True
        io_bytes = metric.get("io_bytes", 0)
        total_io_bytes += io_bytes
        total_iops += metric.get("iops", 0)
        total_bw += metric.get("bw_bytes", 0)
        total_errors += job.get("error", 0)
        if block_size:
            total_samples += io_bytes // block_size
        clat = metric.get("clat_ns", {})
        percentile = clat.get("percentile", {})
        weighted_mean += clat.get("mean", 0) * io_bytes
        max_latency = max(max_latency, clat.get("max", 0))
        p50 += percentile.get("50.000000", 0) * io_bytes
        p95 += percentile.get("95.000000", 0) * io_bytes
        p99 += percentile.get("99.000000", 0) * io_bytes
    if not found:
        return None
    if total_io_bytes > 0:
        weighted_mean /= total_io_bytes
        p50 /= total_io_bytes
        p95 /= total_io_bytes
        p99 /= total_io_bytes
    return {
        "rw": rw,
        "io_bytes": total_io_bytes,
        "iops": total_iops,
        "bw_bytes": total_bw,
        "avg_cost_ms": weighted_mean / 1e6,
        "p50_ms": p50 / 1e6,
        "p95_ms": p95 / 1e6,
        "p99_ms": p99 / 1e6,
        "max_ms": max_latency / 1e6,
        "samples": total_samples,
        "errors": total_errors,
    }


def speed_string(metric):
    return f"{metric['bw_bytes'] / (1024 ** 3):.2f}"


def iops_string(metric):
    return f"{metric['iops']:.2f}"


def progress_string(metric):
    return f"{metric['bw_bytes'] / (1024 ** 3):.2f} GiB/s, {metric['iops']:.2f} IOPS, p99={metric['p99_ms']:.2f}ms"


def build_report(metrics):
    rows = []
    for metric in metrics:
        rows.append([
            f"{metric['item']} ({format_bytes(metric['block_size'])})",
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
    for metric in metrics:
        items.append({
            "item": metric["item"],
            "rw": metric["rw"],
            "block_size": metric["block_size"],
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
    parser.add_argument("--file-size", default=None, help="File size per job; default auto per block size: bs x 65536 IO capped at 1G")
    parser.add_argument("-b", "--bs-list", default=DEFAULT_BLOCK_SIZE, help=f"Comma-separated block sizes, e.g. 4K,64K,1M (default: {DEFAULT_BLOCK_SIZE})")
    parser.add_argument("--iodepth", type=int, default=16, help="Async IO queue depth (default: 16)")
    parser.add_argument("--timeout", type=int, default=300, help="Per-test timeout in seconds, 0 disables (default: 300)")
    parser.add_argument("--ioengine", default="libaio", help="fio ioengine (default: libaio)")
    parser.add_argument("--direct", type=int, default=1, help="O_DIRECT flag, 0 or 1 (default: 1)")
    parser.add_argument("--json", action="store_true", help="Print report as JSON")
    args = parser.parse_args()

    if shutil.which("fio") is None:
        sys.exit("fio not found in PATH, please install fio first")
    if args.threads <= 0:
        sys.exit("--threads must be greater than 0")
    if args.iodepth <= 0:
        sys.exit("--iodepth must be greater than 0")
    if args.timeout < 0:
        sys.exit("--timeout must be greater than or equal to 0")
    if args.direct not in (0, 1):
        sys.exit("--direct must be 0 or 1")
    block_sizes = parse_bs_list(args.bs_list)
    if not block_sizes or any(bs <= 0 for bs in block_sizes):
        sys.exit("--bs-list must contain at least one valid block size")
    file_size = parse_size(args.file_size) if args.file_size else None
    if file_size is not None and file_size <= 0:
        sys.exit("--file-size must be greater than 0")

    directory = ensure_dir(args.directory)
    print()
    print("Configuration: fio")
    if file_size is not None:
        file_size_desc = args.file_size
    else:
        file_size_desc = f"auto (bs x {AUTO_IO_PER_FILE} IO, cap {format_bytes(MAX_FILE_SIZE)})"
    print(
        f"Target: Fuse, Path: {args.directory}, Threads: {args.threads}, "
        f"FileSize: {file_size_desc}, BlockSizes: {args.bs_list}, Iodepth: {args.iodepth}, "
        f"IoEngine: {args.ioengine}, Direct: {args.direct}, Timeout: {args.timeout}s"
    )
    if file_size is not None:
        print(f"Estimated total data per test: {format_bytes(file_size * args.threads)}")
    else:
        print(f"Estimated total data per test: auto per block size, max {format_bytes(MAX_FILE_SIZE * args.threads)}")
    print("Note: each block size uses its own file set (write -> read -> randwrite -> randread)")

    total_tests = len(block_sizes) * len(TESTS)
    metrics = []
    index = 0
    for block_size in block_sizes:
        directory = ensure_dir(directory)
        if file_size is not None:
            size = file_size
        else:
            size = auto_file_size(block_size)
        for rw, item, _ in TESTS:
            index += 1
            print(f"\n[{index}/{total_tests}] {item} ({format_bytes(block_size)}): running ...")
            start = time.time()
            if rw in ("read", "randread"):
                actual = first_data_file_size(directory)
                if actual is not None and actual > 0:
                    size = actual
            data = run_fio(rw, directory, args.threads, size, block_size, args.iodepth, args.timeout, args.ioengine, args.direct)
            metric = extract_metrics(data, rw, block_size)
            if metric is None:
                sys.exit(f"no {rw} metrics in fio output")
            metric["block_size"] = block_size
            metric["item"] = item
            metrics.append(metric)
            print(f"    done in {time.time() - start:.1f}s, {progress_string(metric)}")

    print("\nBenchmark finished!")
    if args.json:
        print(json_report(metrics))
    else:
        for title, rows in build_report(metrics):
            print_table(title, rows)
    print(f"Removing benchmark directory: {directory}")
    shutil.rmtree(directory)
    print(f"Temp path: {directory} (removed)")


if __name__ == "__main__":
    main()
