#!/usr/bin/env python3
"""Compare release binaries in alternating backend order on one ext4 host."""
import argparse
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("worker_bench", HERE / "worker-io-uring-local.py")
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)

MOUNT_SETTINGS = """
[fuse]
io_threads = 8
worker_threads = 8
clone_fd = true
direct_io = true
write_back_cache = false
enable_splice = false
fuse_opts = ["allow_other"]
log = { level = "warn", log_dir = "stderr" }
"""


def count_rings(pid):
    rings = 0
    for fd in Path(f"/proc/{pid}/fd").iterdir():
        try:
            rings += os.readlink(fd) == "anon_inode:[io_uring]"
        except FileNotFoundError:
            continue
    return rings


def get_mount_type(mount):
    query = subprocess.run(["findmnt", "--noheadings", "--output", "FSTYPE", "--mountpoint", str(mount)],
                           capture_output=True, text=True, timeout=5, check=False)
    return query.stdout.strip() if query.returncode == 0 else None


@contextmanager
def mounted(args, state, out, conf, master, worker, record):
    mount = state / "mount"
    mount.mkdir()
    log = out / "fuse.log"
    cmd = ["sudo", "-n", "--", "sh", "-c", 'printf "fuse_pid=%s\\n" "$$"; exec "$@"', "fuse-bench",
           str(args.fuse), "--conf", str(conf), "--mnt-path", str(mount), "--web-port", "0"]
    with log.open("wb") as output:
        proc = subprocess.Popen(cmd, cwd=state, env=bench.child_env(state), start_new_session=True,
                                stdout=output, stderr=subprocess.STDOUT)
    pid = None
    try:
        def ready():
            kind = get_mount_type(mount)
            if kind is None:
                return None
            if kind != "fuse" and not kind.startswith("fuse."):
                raise RuntimeError(f"unexpected mount type {kind}")
            stat = subprocess.run(["stat", "-c", "%F", str(mount)], capture_output=True, text=True,
                                  timeout=5, check=True)
            if stat.stdout.strip() != "directory":
                raise RuntimeError("mount root did not report a directory")
            return kind

        record["mount_type"] = bench.wait_ready("Curvine mount", 20, ready, proc, master, worker)
        log_text = bench.read_text(log)
        match = re.search(r"^fuse_pid=(\d+)$", log_text, re.M)
        if not match or bench.CONFIG_WARNING.search(log_text):
            raise RuntimeError("FUSE mount configuration warnings or missing process ID")
        pid = int(match[1])
        yield mount, SimpleNamespace(pid=pid)
    finally:
        if pid is not None and proc.poll() is None:
            subprocess.run(["sudo", "-n", "--", "kill", "-TERM", str(pid)],
                           capture_output=True, timeout=5, check=False)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        if get_mount_type(mount):
            unmount = ["sudo", "-n", "--", "umount", "--", str(mount)]
            result = subprocess.run(unmount, capture_output=True, text=True, timeout=10, check=False)
            if result.returncode and get_mount_type(mount):
                subprocess.run(unmount[:4] + ["--lazy"] + unmount[4:], timeout=10, check=True)
        try:
            proc.wait(timeout=7)
        except subprocess.TimeoutExpired:
            bench.stop(proc)
        if get_mount_type(mount):
            raise RuntimeError(f"mount still active: {mount}")


def counter(metrics, name):
    values = re.findall(rf"^{name}(?:\{{[^\n]*\}})? (\d+)$", metrics, re.M)
    if not values:
        raise ValueError(f"missing worker metric {name}")
    return sum(map(int, values))


def device_read_bytes(worker):
    return counter(Path(f"/proc/{worker.pid}/io").read_text().replace(": ", " "), "read_bytes")


def measure(args, dest, pair, enabled, cell, ports):
    mode, kib, direction, clients = cell.split("-")
    out = dest / f"{pair}-{cell}-{int(enabled)}"
    out.mkdir()
    conf = out / "cluster.toml"
    text = bench.cluster_toml(ports, enabled)
    text = text.replace('block_size = "1MB"', 'block_size = "128MB"')
    text = text.replace('read_chunk_size = "1MB"', f'read_chunk_size = "{kib}KB"')
    text = text.replace('write_chunk_size = "1MB"', 'write_chunk_size = "128KB"')
    conf.write_text(text.replace("[client]", "[client]\nread_pipeline_depth = 4") + MOUNT_SETTINGS)
    with ExitStack() as stack:
        state = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=out)))
        master, worker = bench.wait_cluster(stack, args.server, conf, state, out, ports, 45)
        report = {}
        processes = [master, worker]
        path = "cv://default/paired"
        if mode == "fuse":
            mount, daemon = stack.enter_context(mounted(args, state, out, conf, master, worker, report))
            processes.append(daemon)
            path = f"file://{mount}/paired"
        budget = os.sched_getaffinity(0)
        for process in processes:
            for tid in Path(f"/proc/{process.pid}/task").iterdir():
                try:
                    if not os.sched_getaffinity(int(tid.name)).issubset(budget):
                        raise RuntimeError("daemon thread exceeds the assigned CPU set")
                except ProcessLookupError:
                    continue
        cli = [str(args.cli), "--conf", str(conf), "--master-addrs", f"{bench.HOST}:{ports.master_rpc}", "bench"]
        common = ["--mode", mode, "--path", path, "--threads", clients, "--block-size", "1MB", "--json"]
        seed = bench.run_cli(cli + ["prefill", "--files", "64", "--file-size", "4MB"] + common,
                             out, state, "prefill", 90, report)
        if seed["errors"] or seed["bytes"] != 256 * bench.MIB:
            raise RuntimeError("invalid prefill")
        cmd = cli + common + ["--workload", f"{direction}_big:100", "--big-file-size", "4MB",
                              "--working-set-files", "64", "--keep-data"]
        warm = bench.run_cli(cmd + ["--duration", "2s"], out, state, "ramp", 60, report)
        if warm["results"][0]["errors"] or not warm["results"][0]["operations"]:
            raise RuntimeError("warmup did not complete cleanly")
        before = bench.scrape(out, ports, "before")
        device = device_read_bytes(worker)
        # Bound new write data on small VMs. Report the actual elapsed interval.
        limit = ["--total-size", "2GB"] if direction == "write" else []
        raw = bench.run_cli(cmd + ["--duration", f"{args.seconds}s"] + limit,
                            out, state, "measure", args.seconds + 90, report, worker)
        device = device_read_bytes(worker) - device
        after = bench.scrape(out, ports, "after")
        bench.check_running(master, worker)
        rings = count_rings(worker.pid)
        if rings != (2 if enabled else 0):
            raise RuntimeError(f"unexpected active rings {rings}")
        row, = raw["results"]
        actual = counter(after, direction + "_bytes") - counter(before, direction + "_bytes")
        requests = counter(after, direction + "_count") - counter(before, direction + "_count")
        ring = bench.ring_deltas(before, after)[direction]["delta_bytes"]
        cfg = raw["config"]
        if (cfg["mode"] != mode or cfg["threads"] != int(clients)
                or cfg["working_set_files"] != 64 or cfg["block_size"] != bench.MIB
                or (direction == "read" and row["elapsed_ms"] < args.seconds * 1000)):
            raise RuntimeError(f"unexpected workload settings in {out}")
        if (row["errors"] or row["op"] != direction + "_big" or not requests
                or row["bytes"] != row["operations"] * 4 * bench.MIB
                or actual != row["bytes"] or ring != actual * enabled):
            raise RuntimeError(f"incomplete payload or unexpected routing in {out}")
        cpu = report["worker_cpu"]
        return dict(pair=pair, cell=cell, enabled=enabled, rings=rings, result=row,
                    cpu_seconds=cpu["user_sec"] + cpu["system_sec"],
                    worker_bytes=actual, data_requests=requests, ring_bytes=ring,
                    non_ring_bytes=actual - ring, process_storage_read_bytes=device)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bin_dir", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--cells", nargs="+", default=[
        "client-4-read-64", "client-128-read-64", "fuse-4-read-64", "client-128-write-1"])
    args = parser.parse_args()
    dest = args.output.resolve()
    dest.mkdir(parents=True)
    args.server, args.cli, args.fuse = [
        args.bin_dir.resolve() / ("curvine-" + name) for name in ("server", "cli", "fuse")]
    hashes = {}
    for path in (args.server, args.cli, args.fuse):
        with path.open("rb") as stream:
            hashes[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    bench.save_json(dest / "manifest.json", dict(binary_sha256=hashes, kernel=os.uname().release,
                    cpu_set=sorted(os.sched_getaffinity(0)), pairs=args.pairs, seconds=args.seconds,
                    cells=args.cells))
    with (dest / "runs.jsonl").open("x") as runs:
        for pair in range(1, args.pairs + 1):
            for i, cell in enumerate(args.cells):
                ports = bench.allocate_ports()
                first = (pair + i) % 2 == 0
                for enabled in (first, not first):
                    row = measure(args, dest, pair, enabled, cell, ports)
                    print(json.dumps(row), file=runs, flush=True)
                    print(pair, cell, enabled, row["result"]["value"], flush=True)


if __name__ == "__main__":
    main()
