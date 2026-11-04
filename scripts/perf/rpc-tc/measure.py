#!/usr/bin/env python3
"""Sample server process CPU or combine a coordinated remote-read run."""
import argparse
import json
import math
import os
import subprocess
import sys


def snapshot(namespace):
    output = subprocess.check_output(["ip", "netns", "pids", namespace], text=True)
    pids = sorted(int(line) for line in output.splitlines() if line.strip())
    if not pids:
        raise ValueError("server namespace has no processes")
    processes = {}
    for pid in pids:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stream:
            stat = stream.read()
        end = stat.rfind(")")
        if end < 0:
            raise ValueError(f"invalid /proc/{pid}/stat")
        fields = stat[end + 1:].split()
        # fields[0] is field 3 (state); field 22 is process starttime.
        if len(fields) < 20:
            raise ValueError(f"truncated /proc/{pid}/stat")
        processes[str(pid)] = {"start": int(fields[19]), "ticks": int(fields[11]) + int(fields[12])}
    return {"ticks_per_second": os.sysconf("SC_CLK_TCK"), "processes": processes}


def finish(client_path, before_path, after_path, output_path, rate, delay, transport, revision):
    def read(path):
        with open(path, encoding="utf-8") as stream:
            return json.load(stream)
    client, before, after = map(read, (client_path, before_path, after_path))
    first, last = before["processes"], after["processes"]
    if not first or first.keys() != last.keys():
        raise ValueError("server process set changed during measurement; CPU accounting would be incomplete")
    if before["ticks_per_second"] != after["ticks_per_second"]:
        raise ValueError("server clock tick rate changed")
    delta = 0
    for pid in first:
        if first[pid]["start"] != last[pid]["start"] or last[pid]["ticks"] < first[pid]["ticks"]:
            raise ValueError(f"server process {pid} restarted or CPU counter decreased")
        delta += last[pid]["ticks"] - first[pid]["ticks"]
    cpu = delta / before["ticks_per_second"]
    if not math.isfinite(cpu):
        raise ValueError("invalid server CPU measurement")
    client["server_cpu_seconds"] = cpu
    client["workload"].update({"rate": rate, "delay_per_direction": delay})
    client["transport"] = transport
    client["revision"] = revision
    client["binaries"] = {"server_sha256": os.environ.get("RPC_TC_SERVER_SHA256", "unverified"),
                          "client_sha256": os.environ.get("RPC_TC_CLIENT_SHA256", "unverified")}
    client["server_process_count"] = len(first)
    with open(output_path, "x", encoding="utf-8") as stream:
        json.dump(client, stream, indent=2, allow_nan=False)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("namespace")
    snap.add_argument("output")
    result = sub.add_parser("finish")
    for name in ("client", "before", "after", "output", "rate", "delay", "transport", "revision"):
        result.add_argument(name)
    args = parser.parse_args()
    try:
        if args.command == "snapshot":
            with open(args.output, "x", encoding="utf-8") as stream:
                json.dump(snapshot(args.namespace), stream)
        else:
            finish(args.client, args.before, args.after, args.output,
                   args.rate, args.delay, args.transport, args.revision)
    except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"benchmark measurement failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
