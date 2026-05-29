#!/usr/bin/env python3
#  // Copyright 2025 OPPO.
#  //
#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  //     http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

"""
Regression test for Curvine FUSE mmap/read coherence (issue #850).

Exercises the open() cache-miss path (keep_cache == false) where the daemon
must not force DIRECT_IO, then verifies MAP_SHARED mmap writes are immediately
visible to pread() on the same fd and persist after close/reopen.

Usage:
    python3 mmap_test.py --dir /curvine-fuse/fuse-test
    python3 mmap_test.py --dir /curvine-fuse/fuse-test --stress
"""

from __future__ import annotations

import argparse
import mmap
import multiprocessing as mp
import os
import sys

DEFAULT_DIR = "/curvine-fuse/fuse-test"
TEST_FILE = "test-mmap-file"
PAGE = 4096
FILE_SIZE = PAGE * 2
MARKER = b"M" * PAGE
STRESS_SIZE = 1 << 20
STRESS_WORKERS = 4
STRESS_ROUNDS = 100


def setup_file(file_path: str, size: int) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    fd = os.open(file_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, size)
        zero = bytes(PAGE)
        for off in range(0, size, PAGE):
            os.pwrite(fd, zero, off)
        os.fsync(fd)
    finally:
        os.close(fd)


def force_cache_miss(file_path: str) -> None:
    """Warm daemon metadata cache, then change backend metadata before reopen."""
    fd = os.open(file_path, os.O_RDONLY)
    try:
        os.pread(fd, 1, 0)
    finally:
        os.close(fd)

    fd = os.open(file_path, os.O_RDWR)
    try:
        end = os.fstat(fd).st_size
        os.pwrite(fd, b"\x01", end)
        os.fsync(fd)
    finally:
        os.close(fd)


def test_mmap_pread_coherence(file_path: str) -> int:
    setup_file(file_path, FILE_SIZE)
    force_cache_miss(file_path)

    fd = os.open(file_path, os.O_RDWR)
    try:
        mm = mmap.mmap(fd, PAGE, access=mmap.ACCESS_WRITE)
        try:
            mm.seek(0)
            mm.write(MARKER)
            # Do not msync/flush — POSIX requires pread to see mmap writes.

            rbuf = os.pread(fd, PAGE, 0)
            if rbuf != MARKER:
                print(
                    "FAIL: mmap write not visible to pread on same fd "
                    f"(expected {MARKER[:8]!r}..., got {rbuf[:8]!r}...)",
                    file=sys.stderr,
                )
                return 1
            print("PASS: mmap write visible to pread on same fd (cache-miss open)")
        finally:
            mm.close()
    finally:
        os.close(fd)

    fd = os.open(file_path, os.O_RDONLY)
    try:
        rbuf = os.pread(fd, PAGE, 0)
        if rbuf != MARKER:
            print(
                "FAIL: mmap write not persisted after close/reopen "
                f"(expected {MARKER[:8]!r}..., got {rbuf[:8]!r}...)",
                file=sys.stderr,
            )
            return 1
        print("PASS: mmap write persisted after close/reopen")
    finally:
        os.close(fd)

    return 0


def _stress_worker(file_path: str, worker_id: int, rounds: int) -> None:
    fd = os.open(file_path, os.O_RDWR)
    try:
        mm = mmap.mmap(fd, STRESS_SIZE, access=mmap.ACCESS_WRITE)
        try:
            marker = ord("A") + worker_id
            block = bytes([marker]) * PAGE
            region = STRESS_SIZE // STRESS_WORKERS

            for r in range(rounds):
                off = (worker_id * region) + ((r * PAGE) % region)
                mm.seek(off)
                mm.write(block)

                rbuf = os.pread(fd, PAGE, off)
                if len(rbuf) != PAGE:
                    print(
                        f"worker={worker_id} round={r} off={off} pread short n={len(rbuf)}",
                        file=sys.stderr,
                    )
                    sys.exit(2)

                for i, b in enumerate(rbuf):
                    if b != marker:
                        print(
                            f"worker={worker_id} round={r} off={off} byte {i}: "
                            f"mmap-wrote='{chr(marker)}' pread-got=0x{b:02x}",
                            file=sys.stderr,
                        )
                        sys.exit(2)
        finally:
            mm.close()
    finally:
        os.close(fd)


def test_mmap_pread_stress(file_path: str, workers: int, rounds: int) -> int:
    setup_file(file_path, STRESS_SIZE)
    force_cache_miss(file_path)

    procs: list[mp.Process] = []
    for w in range(workers):
        p = mp.Process(target=_stress_worker, args=(file_path, w, rounds))
        p.start()
        procs.append(p)

    fail = 0
    for p in procs:
        p.join()
        if p.exitcode != 0:
            print(f"worker exit status={p.exitcode}", file=sys.stderr)
            fail += 1

    if fail == 0:
        print(f"PASS: {workers} workers x {rounds} rounds mmap/pread stress test")
        return 0

    print(
        f"FAIL: {fail}/{workers} workers reported mmap/read inconsistency",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Regression test for Curvine FUSE mmap/read coherence (#850)",
    )
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Directory for test files")
    parser.add_argument(
        "--stress",
        action="store_true",
        help="Run concurrent mmap+pread stress test (issue #850 reproducer)",
    )
    parser.add_argument("--workers", type=int, default=STRESS_WORKERS)
    parser.add_argument("--rounds", type=int, default=STRESS_ROUNDS)
    args = parser.parse_args()

    file_path = os.path.join(os.path.abspath(args.dir), TEST_FILE)

    try:
        os.unlink(file_path)
    except FileNotFoundError:
        pass

    if args.stress:
        rc = test_mmap_pread_stress(file_path, args.workers, args.rounds)
    else:
        rc = test_mmap_pread_coherence(file_path)

    try:
        os.unlink(file_path)
    except FileNotFoundError:
        pass

    return rc


if __name__ == "__main__":
    sys.exit(main())
