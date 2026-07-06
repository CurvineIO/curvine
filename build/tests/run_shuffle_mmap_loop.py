#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import time


MAIN_CLASS = "io.curvine.SparkShuffleTransferToMmapTest"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("jar", nargs="?", default="curvine-hadoop-0.2.0-shade.jar:curvine-hadoop-0.2.0-tests.jar")
    parser.add_argument("data_dir", nargs="?", default="/curvine-fuse/spark-disk")
    parser.add_argument("loop_count", nargs="?", type=int, default=10000)
    parser.add_argument("parallelism", nargs="?", type=int, default=64)
    parser.add_argument("--progress-seconds", type=int, default=10)
    return parser.parse_args()


def start_one(run_id, jar, data_dir):
    print("===== run {} start =====".format(run_id), flush=True)
    cmd = [
        "java",
        "-Djava.io.tmpdir={}".format(data_dir),
        "-XX:ErrorFile=/dev/stdout",
        "-cp",
        jar,
        MAIN_CLASS,
        data_dir,
    ]
    proc = subprocess.Popen(cmd, stdout=None, stderr=None)
    print("===== run {} pid {} =====".format(run_id, proc.pid), flush=True)
    return proc


def main():
    args = parse_args()
    os.makedirs(args.data_dir, exist_ok=True)

    print("jar: {}".format(args.jar), flush=True)
    print("data dir: {}".format(args.data_dir), flush=True)
    print("loop count: {}".format(args.loop_count), flush=True)
    print("parallelism: {}".format(args.parallelism), flush=True)
    print("progress seconds: {}".format(args.progress_seconds), flush=True)

    for batch_start in range(1, args.loop_count + 1, args.parallelism):
        batch_end = min(batch_start + args.parallelism - 1, args.loop_count)
        print("===== batch {}-{}/{} =====".format(batch_start, batch_end, args.loop_count), flush=True)

        running = {}
        for run_id in range(batch_start, batch_end + 1):
            running[run_id] = start_one(run_id, args.jar, args.data_dir)

        last_progress = time.time()
        while running:
            for run_id, proc in list(running.items()):
                status = proc.poll()
                if status is None:
                    continue

                print("===== run {} pid {} exit {} =====".format(run_id, proc.pid, status), flush=True)
                del running[run_id]

                if status != 0:
                    print("run {} failed with status {}".format(run_id, status), flush=True)
                    return status

            now = time.time()
            if running and now - last_progress >= args.progress_seconds:
                waiting = ["{}:{}".format(run_id, proc.pid) for run_id, proc in sorted(running.items())]
                print("waiting runs: {}".format(" ".join(waiting)), flush=True)
                print("check with: ps -fp {}".format(",".join(str(proc.pid) for proc in running.values())), flush=True)
                last_progress = now

            time.sleep(0.2)

    print("all {} runs passed".format(args.loop_count), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
