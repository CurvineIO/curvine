#!/usr/bin/env python3
# Copyright 2026 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Curvine #791: paired Linux loopback benchmark with prebuilt local binaries.

Warm-page-cache recipe: read directly after writes (throughput), or prefill
then immediately reuse the working set (mixed_throughput). No cache eviction,
forced sync, or writeback settling. A warm recipe does not guarantee residency.
This one-host run includes TCP, master, client, and worker contention;
it is neither a cold disk test nor a production capacity/tail-latency result.
"""

import argparse
import http.client
import json
import os
import platform
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple

HOST = "127.0.0.1"
MIB = 1 << 20
BLOCK = "1MB"  # Curvine's ByteUnit spells 1 MiB as 1MB.
CONCURRENCIES = (1, 16)
WORKLOADS = ("throughput", "mixed_throughput")
CV_PATH = "cv://default/__worker_io_uring_bench__"
BACKEND_LOG = re.compile(
    r"\brequested=(local_file|io_uring)[,;]?\s+effective=(local_file|io_uring)\b"
)
CONFIG_WARNING = re.compile(
    r"failed to load config file|using default configuration|unrecognized config key",
    re.I,
)


class Ports(NamedTuple):
    master_rpc: int
    master_web: int
    journal_rpc: int
    worker_rpc: int
    worker_web: int


def allocate_ports():
    # Keep reservations distinct, then hand the ports to the foreground daemons.
    # Like the subprocess follower tests, there is a small release-to-bind race.
    with ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in Ports._fields]
        for sock in listeners:
            sock.bind((HOST, 0))
        return Ports(*(sock.getsockname()[1] for sock in listeners))


def cluster_toml(ports, enabled):
    # Paths are relative to a new private cwd in each arm. For a pair the
    # addresses, settings, and namespace are identical except for the switch.
    return f'''cluster_id = "default"
format_master = false
format_worker = false
testing = false

[master]
hostname = "{HOST}"
rpc_port = {ports.master_rpc}
web_port = {ports.master_web}
meta_dir = "meta"
io_threads = 4
worker_threads = 8
log = {{ level = "info", log_dir = "stderr" }}

[journal]
hostname = "{HOST}"
rpc_port = {ports.journal_rpc}
journal_addrs = [{{ id = 1, hostname = "{HOST}", port = {ports.journal_rpc} }}]
journal_dir = "journal"
io_threads = 2
worker_threads = 4
# Use the localhost integration test tick to shorten fresh single-node elections.
raft_tick_interval_ms = 100

[worker]
hostname = "{HOST}"
rpc_port = {ports.worker_rpc}
web_port = {ports.worker_web}
data_dir = ["[DISK]blocks"]
dir_reserved = "0"
io_threads = 4
worker_threads = 16
enable_send_file = false
enable_splice = false
block_checksum = false
block_checksum_verify = false
enable_s3_gateway = false
enable_io_uring = {str(enabled).lower()}
log = {{ level = "info", log_dir = "stderr" }}

[worker.spdk_disk]
enabled = false

[client]
hostname = "{HOST}"
master_addrs = [{{ hostname = "{HOST}", port = {ports.master_rpc} }}]
replicas = 1
short_circuit = false
enable_read_ahead = false
block_size = "{BLOCK}"
read_chunk_size = "{BLOCK}"
write_chunk_size = "{BLOCK}"
io_threads = 4
worker_threads = 8

[cli]
log = {{ level = "warn", log_dir = "stderr" }}

[discovery]
enabled = false

[transfer]
enabled = false
'''


def child_env(state):
    # Don't inherit cluster/network/proxy overrides or discover installed config.
    return {
        "PATH": os.defpath,
        "HOME": str(state),
        "TMPDIR": str(state),
        "ORPC_BIND_HOSTNAME": HOST,
        "LC_ALL": "C",
        "TZ": "UTC",
    }


def read_text(path):
    return path.read_text(encoding="utf-8", errors="replace")


def save_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def http_get(port, path):
    # Direct loopback traffic: no proxy, redirect, or DNS lookup.
    conn = http.client.HTTPConnection(HOST, port, timeout=1)
    try:
        conn.request("GET", path, headers={"Connection": "close"})
        with conn.getresponse() as response:
            if response.status != 200:
                raise OSError(f"{HOST}:{port}{path} returned HTTP {response.status}")
            return response.read().decode("utf-8")
    finally:
        conn.close()


def check_running(*services):
    for service in services:
        status = service.poll()
        if status is not None:
            raise RuntimeError(
                f"{service.args[2]} exited with status {status}; see service logs"
            )


def stop(service):
    if service.poll() is None:
        service.terminate()
        try:
            service.wait(timeout=5)
        except subprocess.TimeoutExpired:
            service.kill()
            service.wait()


def start_service(stack, server, service, conf, state, log):
    with log.open("wb") as output:
        proc = subprocess.Popen(
            [str(server), "--service", service, "--conf", str(conf)],
            cwd=state,
            env=child_env(state),
            start_new_session=True,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    stack.callback(stop, proc)
    return proc


def wait_ready(description, timeout, probe, *services):
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        check_running(*services)
        try:
            result = probe()
            if result is not None:
                check_running(*services)
                return result
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {description}: {last_error}")


def wait_cluster(stack, server, conf, state, out, ports, timeout):
    master_log, worker_log = out / "master.log", out / "worker.log"
    master = start_service(stack, server, "master", conf, state, master_log)

    def workers():
        return json.loads(http_get(ports.master_web, "/api/workers"))

    # The master starts its web service after Raft role selection and RPC bind.
    empty = wait_ready("master", timeout, workers, master)
    if empty["live_workers"] or empty["lost_workers"]:
        raise RuntimeError("fresh master already has registered workers")
    worker = start_service(stack, server, "worker", conf, state, worker_log)

    def online():
        data = workers()
        live = data["live_workers"]
        if not live:
            return None
        entry = live[0]
        addr = entry["address"]
        if (
            len(live) != 1
            or data["lost_workers"]
            or entry["status"] != "Live"
            or addr["hostname"] != HOST
            or addr["rpc_port"] != ports.worker_rpc
        ):
            raise RuntimeError(f"unexpected registration in the fresh cluster: {data}")
        return data

    save_json(
        out / "workers.json",
        wait_ready("one live worker", timeout, online, master, worker),
    )
    for log in (master_log, worker_log):
        if CONFIG_WARNING.search(read_text(log)):
            raise RuntimeError(f"configuration warning in {log.name}")
    return master, worker


def scrape(out, ports, phase):
    text = http_get(ports.worker_web, "/metrics")
    (out / f"{phase}.prom").write_text(text, encoding="utf-8")
    return text


def ring_deltas(before, after):
    evidence = {}
    for direction in ("read", "write"):
        base = f"io_uring_{direction}_bytes"
        # WorkerMetrics normally registers unprefixed names; accept the planned
        # worker_ prefix and the Prometheus _total convention. Don't double-count.
        sample = re.compile(
            rf"^((?:worker_)?{base}(?:_total)?)(\{{.*\}})?[ \t]+(\S+)", re.M
        )

        def series(text):
            families = {}
            for name, labelset, raw in sample.findall(text):
                value = Decimal(raw)
                values = families.setdefault(name, {})
                if (
                    labelset in values
                    or not value.is_finite()
                    or value < 0
                    or value != value.to_integral_value()
                ):
                    raise ValueError(f"invalid counter sample: {name}{labelset} {raw}")
                values[labelset] = int(value)
            return families

        a, b = series(before), series(after)
        names = set(a) | set(b)
        if len(names) != 1:
            evidence[direction] = {
                "issue": "missing or ambiguous metric family",
                "families": sorted(names),
            }
            continue
        name = names.pop()
        old, new = a.get(name, {}), b.get(name, {})
        stats = {
            "metric": name,
            "before_bytes": sum(old.values()) if old else None,
            "after_bytes": sum(new.values()) if new else None,
        }
        if not old or not new:
            stats["issue"] = "need samples in both scrapes (missing is not zero)"
        elif any(new.get(labels, -1) < value for labels, value in old.items()):
            stats["issue"] = "counter decreased or a series disappeared"
        else:
            stats["delta_bytes"] = stats["after_bytes"] - stats["before_bytes"]
        evidence[direction] = stats
    return evidence


def cpu_sample(out, worker, phase):
    raw = read_text(Path(f"/proc/{worker.pid}/stat"))
    (out / f"{phase}.stat").write_text(raw, encoding="utf-8")
    fields = raw.rsplit(")", 1)[
        1
    ].split()  # starts at field 3; comm can contain spaces/parentheses
    return int(fields[11]), int(fields[12]), int(fields[19])  # utime, stime, starttime


def run_cli(cmd, out, state, phase, timeout, record, worker=None):
    stdout, stderr = out / f"{phase}.json", out / f"{phase}.stderr.log"
    record[phase + "_argv"] = cmd
    with stdout.open("wb") as output, stderr.open("wb") as errors:
        if worker is not None:
            u0, s0, birth = cpu_sample(out, worker, "before")
            start = time.monotonic()
        try:
            proc = subprocess.run(
                cmd,
                cwd=state,
                env=child_env(state),
                start_new_session=True,
                stdout=output,
                stderr=errors,
                timeout=timeout,
                check=False,
            )
        finally:
            if worker is not None and worker.poll() is None:
                elapsed = time.monotonic() - start
                u1, s1, end_birth = cpu_sample(out, worker, "after")
                if birth != end_birth or u1 < u0 or s1 < s0:
                    raise RuntimeError(
                        "worker process identity or CPU stat changed unexpectedly"
                    )
                hz = os.sysconf("SC_CLK_TCK")
                record["worker_cpu"] = {
                    "pid": worker.pid,
                    "clock_ticks_per_sec": hz,
                    "user_ticks": u1 - u0,
                    "system_ticks": s1 - s0,
                    "user_sec": (u1 - u0) / hz,
                    "system_sec": (s1 - s0) / hz,
                    "window_wall_sec": elapsed,
                }
    record[phase + "_exit"] = proc.returncode
    if proc.returncode or CONFIG_WARNING.search(read_text(stderr)):
        raise RuntimeError(f"{phase} failed or did not honor config; see {stderr.name}")
    return json.loads(read_text(stdout))


def benchmark(
    args, state, out, conf, ports, workload, threads, master, worker, startup, record
):
    # Pin the master even if the CLI falls back to defaults; such a run is
    # invalid, but must still never talk to any other Curvine instance.
    cli = [
        str(args.cli),
        "--conf",
        str(conf),
        "--master-addrs",
        f"{HOST}:{ports.master_rpc}",
        "bench",
    ]
    common = [
        "--mode",
        "client",
        "--path",
        CV_PATH,
        "--block-size",
        BLOCK,
        "--threads",
        str(threads),
        "--files-per-dir",
        "1000",
        "--json",
    ]
    file_size = f"{args.file_mib}MB"
    mixed = workload == "mixed_throughput"
    if mixed:
        seed = run_cli(
            cli
            + ["prefill", "--files", str(args.files), "--file-size", file_size]
            + common,
            out,
            state,
            "prefill",
            args.timeout,
            record,
        )
        if (
            seed["errors"]
            or seed["operations"] != args.files
            or seed["bytes"] != args.files * args.file_mib * MIB
        ):
            raise RuntimeError(
                "prefill did not write the requested working set; see prefill.json"
            )
    check_running(master, worker)
    before = scrape(out, ports, "before") if mixed else startup
    if mixed:
        record["prefill_ring_bytes"] = ring_deltas(startup, before)

    cmd = (
        cli
        + common
        + [
            "--workload",
            workload,
            "--big-file-size",
            file_size,
            "--big-file-count",
            str(args.files // threads),
            "--keep-data",
        ]
    )
    if mixed:
        cmd += [
            "--working-set-files",
            str(args.files),
            "--duration",
            f"{args.mixed_seconds}s",
            "--total-size",
            f"{args.mixed_mib}MB",
        ]
    report = run_cli(cmd, out, state, "bench", args.timeout, record, worker)
    check_running(master, worker)
    record["ring_bytes"] = ring_deltas(before, scrape(out, ports, "after"))
    config = report["config"]
    if (
        config["mode"] != "client"
        or config["block_size"] != MIB
        or config["threads"] != threads
        or config["working_set_files"] != (args.files if mixed else None)
    ):
        raise RuntimeError("unexpected benchmark configuration; see bench.json")
    rows = report["results"]
    if len(rows) != 2 or {row["op"] for row in rows} != {"read_big", "write_big"}:
        raise RuntimeError("missing measured read/write operations; see bench.json")
    for row in rows:
        if (
            row["group"] != workload
            or row["errors"]
            or not row["operations"]
            or row["bytes"] != row["operations"] * args.file_mib * MIB
            or (not mixed and row["operations"] != args.files)
        ):
            raise RuntimeError(
                f"incomplete or failed {row['op']} operations; see bench.json"
            )
    record["results"] = (
        rows  # per-op MiB/s, bytes, errors, whole-file latencies, latency_samples
    )


def run_arm(args, out, ports, workload, threads, enabled):
    out.mkdir()
    conf = out / "cluster.toml"
    conf.write_text(cluster_toml(ports, enabled), encoding="utf-8")
    requested = "io_uring" if enabled else "local_file"
    record = {
        "requested": requested,
        "effective": None,
        "status": "failed",
        "workload": workload,
        "threads": threads,
    }
    try:
        seed_mib = args.files * args.file_mib
        peak_mib = seed_mib + (
            args.mixed_mib + threads * args.file_mib if workload == WORKLOADS[1] else 0
        )
        if shutil.disk_usage(out).free < (peak_mib + 512) * MIB:
            raise RuntimeError(
                f"need ~{peak_mib + 512} MiB free (data upper bound plus 512 MiB headroom)"
            )
        with ExitStack() as stack:
            state = Path(
                stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="state-", dir=out)
                )
            )
            master, worker = wait_cluster(
                stack, args.server, conf, state, out, ports, args.startup_timeout
            )
            selection = [
                (line, BACKEND_LOG.search(line))
                for line in read_text(out / "worker.log").splitlines()
            ]
            selection = [(line, match) for line, match in selection if match]
            if len(selection) != 1:
                raise RuntimeError(
                    "expected one requested/effective backend diagnostic; see worker.log"
                )
            line, match = selection[0]
            logged_request, effective = match.groups()
            record.update(effective=effective, backend_log=line)
            if logged_request != requested:
                raise RuntimeError(
                    f"worker requested {logged_request}, expected {requested}"
                )
            startup = scrape(
                out, ports, "startup"
            )  # after probe/registration, before any benchmark I/O
            if effective != requested:
                record.update(status="fallback", reason=line)
            else:
                benchmark(
                    args,
                    state,
                    out,
                    conf,
                    ports,
                    workload,
                    threads,
                    master,
                    worker,
                    startup,
                    record,
                )
                # All data in this preset is eligible. A few successful SQEs
                # are not evidence that the measured workload used the ring.
                verified = all(
                    record["ring_bytes"][row["op"].removesuffix("_big")].get(
                        "delta_bytes"
                    )
                    == (row["bytes"] if enabled else 0)
                    for row in record["results"]
                )
                record["status"] = "verified" if verified else "unverified"
                if not verified:
                    record["reason"] = (
                        "expected ring-byte deltas to equal measured read/write bytes when on, zero when off"
                    )
        # ExitStack reaps worker/master before removing the whole private state,
        # including --keep-data and prefill, even when the benchmark fails.
    except (
        OSError,
        ValueError,
        KeyError,
        ArithmeticError,
        RuntimeError,
        subprocess.SubprocessError,
        http.client.HTTPException,
    ) as exc:
        record.update(status="failed", reason=str(exc))
    finally:
        save_json(out / "trial.json", record)
    return record


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def main():
    target = Path(__file__).resolve().parents[2] / "target"
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--server", type=Path, default=target / "debug/curvine-server")
    parser.add_argument(
        "--cli",
        type=Path,
        default=target / "debug/curvine-cli",
        help="the binary behind cv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="new results directory under target/ (default: unique directory)",
    )
    parser.add_argument(
        "--repetitions",
        type=positive_int,
        default=2,
        help="off/on pairs per case (default: 2)",
    )
    parser.add_argument(
        "--files",
        type=positive_int,
        default=64,
        help="total files, multiple of 16 (default: 64)",
    )
    parser.add_argument(
        "--file-mib", type=positive_int, default=4, help="MiB per big file (default: 4)"
    )
    parser.add_argument(
        "--mixed-mib",
        type=positive_int,
        default=512,
        help="mixed combined byte limit in MiB (default: 512)",
    )
    parser.add_argument(
        "--mixed-seconds",
        type=positive_int,
        default=30,
        help="mixed duration limit (default: 30)",
    )
    parser.add_argument(
        "--startup-timeout",
        type=positive_int,
        default=60,
        help="seconds per readiness check (default: 60)",
    )
    parser.add_argument(
        "--timeout",
        type=positive_int,
        default=180,
        help="seconds per CLI command (default: 180)",
    )
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("Linux /proc is required")
    if args.files % max(CONCURRENCIES):
        parser.error(
            "--files must be a multiple of 16 to keep the total constant across concurrencies"
        )
    for name in ("server", "cli"):
        binary = getattr(args, name).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(
                f"missing or non-executable --{name}: {binary}; supply a prebuilt local binary"
            )
        setattr(args, name, binary)

    target.mkdir(exist_ok=True)
    target = target.resolve()
    if args.output is None:
        out = Path(tempfile.mkdtemp(prefix="worker-io-uring-", dir=target))
    else:
        out = args.output.resolve()
        if target not in out.parents or out.exists():
            parser.error("--output must be a new directory under target/")
        out.mkdir(parents=True)
    print(f"Results: {out}", flush=True)

    # Don't let a debug-binary crash create a multi-GB core file on this disk.
    resource.setrlimit(
        resource.RLIMIT_CORE, (0, resource.getrlimit(resource.RLIMIT_CORE)[1])
    )
    save_json(
        out / "run.json",
        {
            "argv": sys.argv,
            "settings": {
                key: str(val) if isinstance(val, Path) else val
                for key, val in vars(args).items()
            },
            "binaries_mtime_ns": {
                key: getattr(args, key).stat().st_mtime_ns for key in ("server", "cli")
            },
            "host": platform.uname()._asdict(),
            "available_cpus": len(os.sched_getaffinity(0)),
            "memory": read_text(Path("/proc/meminfo")).splitlines()[:3],
            "initial_free_bytes": shutil.disk_usage(out).free,
            "block_and_chunk_bytes": MIB,
            "limitations": __doc__.strip(),
            "measurement": "Whole CLI command CPU (excludes startup, separate prefill, and teardown); "
            "worker process ticks exclude work charged to other kernel tasks. "
            "Ring deltas span startup.prom -> after.prom for throughput, "
            "before.prom -> after.prom for mixed (prefill reported separately). "
            "Mixed work can overshoot with in-flight files. Small whole-file latency samples "
            "make p95/p99 noisy; compare only verified matched pairs.",
        },
    )
    pairs = []
    for rep in range(1, args.repetitions + 1):
        for workload in WORKLOADS:
            for threads in CONCURRENCIES:
                pair = out / f"{rep:02d}-{workload}-p{threads}"
                pair.mkdir()
                ports = allocate_ports()
                arms = []
                pairs.append({"pair": pair.name, "arms": arms})
                for enabled in (False, True) if rep % 2 else (True, False):
                    arm = pair / ("on" if enabled else "off")
                    record = run_arm(args, arm, ports, workload, threads, enabled)
                    arms.append(
                        {
                            "artifact_dir": str(arm.relative_to(out)),
                            "status": record["status"],
                        }
                    )
                    save_json(out / "summary.json", pairs)
                    ring = record.get("ring_bytes", {})
                    deltas = ", ".join(
                        f"{op}={ring.get(op, {}).get('delta_bytes')}"
                        for op in ("read", "write")
                    )
                    detail = record.get(
                        "reason", f"completed ring byte deltas: {deltas}"
                    )
                    print(
                        f"{pair.name}/{arm.name}: {record['status']} — {detail}",
                        flush=True,
                    )
    return int(
        any(arm["status"] != "verified" for pair in pairs for arm in pair["arms"])
    )


def interrupt(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupt)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit("Interrupted; completed artifacts remain in the results directory")
