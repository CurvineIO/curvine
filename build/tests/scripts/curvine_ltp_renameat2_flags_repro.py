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
renameat2(2) extended flags not implemented on Curvine FUSE — reproducer for LTP failures.

Root cause: Curvine FUSE rejects RENAME_EXCHANGE and RENAME_NOREPLACE renameat2(2)
operations that should succeed, returning EINVAL instead of performing the atomic
rename or exchange.

Covered LTP cases (each line is one minimal repro scenario):
  renameat201 — RENAME_EXCHANGE with both paths present must succeed (TFAIL test 2)
  renameat201 — RENAME_NOREPLACE with absent destination must succeed (TFAIL test 4)
  renameat202 — RENAME_EXCHANGE must succeed to atomically swap two existing files (TFAIL)

Usage:
    python3 curvine_ltp_renameat2_flags_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_renameat2_flags_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import os
import platform
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

RENAME_NOREPLACE = 1 << 0
RENAME_EXCHANGE = 1 << 1


def expect_ok(name: str, fn: Callable[[], None]) -> int:
    try:
        fn()
        print(f"  PASS: {name}")
        return 0
    except OSError as exc:
        print(
            f"  FAIL: {name} — unexpected OSError errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — unexpected error: {exc}", file=sys.stderr)
        return 1


def expect_errno(name: str, fn: Callable[[], None], expected: int) -> int:
    try:
        fn()
        print(
            f"  FAIL: {name} — expected errno {expected} "
            f"({errno.errorcode.get(expected, '?')}), but syscall succeeded",
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        if exc.errno == expected:
            print(
                f"  PASS: {name} (errno={expected} "
                f"{errno.errorcode.get(expected, '?')})"
            )
            return 0
        print(
            f"  FAIL: {name} — expected errno {expected}, got {exc.errno}: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — expected OSError, got {exc}", file=sys.stderr)
        return 1


def skip(name: str, reason: str) -> int:
    print(f"  SKIP: {name} — {reason}")
    return 0


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def _syscall_nr_renameat2() -> int:
    table = {
        "x86_64": 316,
        "aarch64": 276,
    }
    machine = platform.machine()
    if machine not in table:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for renameat2: {machine}",
        )
    return table[machine]


def sys_renameat2(
    olddirfd: int,
    oldpath: str,
    newdirfd: int,
    newpath: str,
    flags: int,
) -> None:
    """renameat2(olddirfd, oldpath, newdirfd, newpath, flags) via raw syscall."""
    libc = ctypes.CDLL(None, use_errno=True)
    syscall_fn = libc.syscall
    syscall_fn.restype = ctypes.c_long

    ret = int(
        syscall_fn(
            _syscall_nr_renameat2(),
            olddirfd,
            ctypes.c_char_p(oldpath.encode()),
            newdirfd,
            ctypes.c_char_p(newpath.encode()),
            flags,
        )
    )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def _open_dir(path: str) -> int:
    # Curvine FUSE rejects open(2) with O_DIRECTORY (EINVAL); O_RDONLY still
    # yields a valid directory fd for renameat2(2) dirfd arguments.
    return os.open(path, os.O_RDONLY)


def test_rename_exchange_both_exist(root: str) -> int:
    # Related LTP cases: renameat201, renameat202
    # renameat2(2) with RENAME_EXCHANGE must succeed when both source and destination exist.
    workdir = case_dir(root, "rename_exchange")
    dir1 = os.path.join(workdir, "test_dir")
    dir2 = os.path.join(workdir, "test_dir2")
    os.makedirs(dir1, mode=0o700)
    os.makedirs(dir2, mode=0o700)

    old_file = os.path.join(dir1, "test_file")
    new_file = os.path.join(dir2, "test_file2")
    with open(old_file, "w", encoding="utf-8") as fh:
        fh.write("content")
    open(new_file, "w", encoding="utf-8").close()

    olddirfd = _open_dir(dir1)
    newdirfd = _open_dir(dir2)
    try:

        def run_exchange() -> None:
            sys_renameat2(
                olddirfd,
                "test_file",
                newdirfd,
                "test_file2",
                RENAME_EXCHANGE,
            )

        return expect_ok(
            "renameat2(RENAME_EXCHANGE) succeeds when both paths exist "
            "(renameat201 test2, renameat202)",
            run_exchange,
        )
    finally:
        os.close(olddirfd)
        os.close(newdirfd)


def test_rename_noreplace_dest_absent_must_succeed(root: str) -> int:
    # Related LTP cases: renameat201
    # renameat2(2) with RENAME_NOREPLACE must succeed when the destination path is absent.
    workdir = case_dir(root, "rename_noreplace")
    dir1 = os.path.join(workdir, "test_dir")
    dir2 = os.path.join(workdir, "test_dir2")
    os.makedirs(dir1, mode=0o700)
    os.makedirs(dir2, mode=0o700)

    old_file = os.path.join(dir1, "test_file")
    open(old_file, "w", encoding="utf-8").close()

    olddirfd = _open_dir(dir1)
    newdirfd = _open_dir(dir2)
    try:

        def run_noreplace() -> None:
            sys_renameat2(
                olddirfd,
                "test_file",
                newdirfd,
                "test_file3",
                RENAME_NOREPLACE,
            )

        return expect_ok(
            "renameat2(RENAME_NOREPLACE) succeeds when destination is absent "
            "(renameat201 test4)",
            run_noreplace,
        )
    finally:
        os.close(olddirfd)
        os.close(newdirfd)


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "RENAME_EXCHANGE succeeds when both files exist (renameat201/202)",
        test_rename_exchange_both_exist,
    ),
    (
        "2",
        "RENAME_NOREPLACE succeeds when destination absent (renameat201)",
        test_rename_noreplace_dest_absent_must_succeed,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="renameat2 extended flags not implemented on Curvine FUSE — reproducer",
    )
    parser.add_argument(
        "--dir",
        default=DEFAULT_DIR,
        help="Base test directory (must be on the Curvine FUSE mount)",
    )
    parser.add_argument(
        "--allow-non-mount",
        action="store_true",
        help="Skip FUSE mount check; for local debugging only (default: False)",
    )
    args = parser.parse_args()

    if not sys.platform.startswith("linux"):
        print("SKIP: Linux-only syscalls", file=sys.stderr)
        return 0

    if os.geteuid() != 0:
        print("WARNING: not running as root", file=sys.stderr)

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if args.allow_non_mount:
        print(
            "WARNING: --allow-non-mount set; results may not reflect Curvine FUSE behaviour",
            file=sys.stderr,
        )

    print(f"Running renameat2-flags reproducer under {root}\n")

    fail = 0
    for num, title, test_fn in TESTS:
        print(f"[case {num}] {title}:")
        fail += test_fn(root)
        print()

    total = len(TESTS)
    passed = total - fail
    print(f"Summary: {passed}/{total} passed, {fail}/{total} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
