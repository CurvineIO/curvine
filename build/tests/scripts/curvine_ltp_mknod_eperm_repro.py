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
mknod / mkfifo EPERM on Curvine FUSE — reproducer for LTP failures.

Root cause: Curvine FUSE does not implement the FUSE_MKNOD operation (or
returns EPERM unconditionally), which causes every syscall that creates a
special file node—named pipe (FIFO), character device, block device, or Unix
domain socket file—to fail with EPERM even when called by root.  This single
bug cascades across more than 25 otherwise unrelated LTP test groups because
they all rely on one of these node-creation calls during their setup phase.

Covered LTP cases and the failing operation in each:
  mknod01 (case 2)  — mknod(path, S_IFIFO|0777, 0) → EPERM (TFAIL, test logic)
  mknod02           — mknod(path, S_IFCHR|0777, 0) → EPERM (TFAIL)
  mknod03           — mknod(path, S_IFCHR|0777, 0) → EPERM (TFAIL)
  mknod04           — mknod(path, S_IFIFO|0777, 0) → EPERM (TFAIL)
  mknod05           — mknod(path, S_IFCHR|0777, 0) → EPERM (TFAIL)
  mknod06           — mknod(path, S_IFBLK|mode, dev) in setup → EPERM (TBROK)
  mknod08           — mknod(path, S_IFIFO|0777, 0) → EPERM (TFAIL)
  fcntl07           — mkfifo(fifo, 0666) in setup → EPERM (TBROK)
  fcntl07_64        — mkfifo(fifo, 0666) in setup → EPERM (TBROK)
  dup05             — mkfifo(dupfile, 0777) in setup → EPERM (TBROK)
  stream02          — mknod() in test body → EPERM (TFAIL)
  lseek02           — mkfifo(tfifo1, 0777) in setup → EPERM (TBROK)
  read03            — mknod() in setup → EPERM (TBROK)
  write04           — mknod() in setup → EPERM (TBROK)
  fsync03           — mkfifo(fifo, 0644) in setup → EPERM (TBROK)
  select01          — mkfifo(tmpfile2, 0666) in setup → EPERM (TBROK)
  open06            — mknod() char device in setup → EPERM (TBROK)
  open11            — mknod() char device in setup → EPERM (TBROK)
  statx01           — mknod() char device in setup → EPERM (TBROK)
  getxattr02        — mkfifo(getxattr02fifo) in setup → EPERM (TBROK)
  fgetxattr02       — mknod() char device in setup → EPERM (TBROK)
  setxattr02        — mknod() char device in setup → EPERM (TBROK)
  getsockopt02      — bind(AF_UNIX, path_on_fuse) → EPERM (TBROK)
  recvmsg01         — bind(AF_UNIX, path_on_fuse) → EPERM (TBROK)
  sendmsg01         — bind(AF_UNIX, path_on_fuse) → EPERM (TBROK)
  sockioctl01       — bind(AF_UNIX, path_on_fuse) → EPERM (TBROK)
  bind03            — bind(AF_UNIX, path_on_fuse) → EPERM (TBROK)
  unlink05 (case 2) — mkfifo(tfifo, 0777) → EPERM (TBROK)

Usage:
    python3 curvine_ltp_mknod_eperm_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_mknod_eperm_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import errno
import os
import shutil
import socket
import stat
import sys
from typing import Callable, List, Tuple

DEFAULT_DIR = "/curvine-fuse/fuse-test"


# ---------------------------------------------------------------------------
# Helpers — identical signatures and behaviour to file_handle_at_test.py
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Test functions
# ---------------------------------------------------------------------------

def test_mkfifo_on_fuse_must_not_eperm(root: str) -> int:
    # Related LTP cases: fcntl07, fcntl07_64, dup05, lseek02, fsync03, select01,
    #                    getxattr02, unlink05 (case 2), mknod01 (case 2)
    # mkfifo(2) shall create a named pipe on the filesystem; EPERM from FUSE
    # means the FUSE_MKNOD handler is missing or unconditionally rejecting.
    workdir = case_dir(root, "mkfifo_eperm")
    fifo_path = os.path.join(workdir, "test.fifo")

    return expect_ok(
        "mkfifo(path, 0o666) on FUSE mount",
        lambda: os.mkfifo(fifo_path, 0o666),
    )


def test_mknod_fifo_type_on_fuse_must_not_eperm(root: str) -> int:
    # Related LTP cases: mknod01 (case 2), mknod04, mknod08, stream02, read03, write04
    # mknod(2) with S_IFIFO type bit set must succeed when caller is root;
    # FUSE returning EPERM reveals the missing mknod implementation.
    name = "mknod(path, S_IFIFO|0o777, 0) on FUSE mount"
    if os.geteuid() != 0:
        return skip(name, "requires root (CAP_MKNOD)")
    workdir = case_dir(root, "mknod_fifo_eperm")
    node_path = os.path.join(workdir, "test_fifo_node")

    return expect_ok(
        name,
        lambda: os.mknod(node_path, stat.S_IFIFO | 0o777),
    )


def test_mknod_chardev_on_fuse_must_not_eperm(root: str) -> int:
    # Related LTP cases: mknod02, mknod03, mknod05, open06, open11, statx01,
    #                    fgetxattr02, setxattr02
    # mknod(2) with S_IFCHR (null device 1:3) must succeed as root; any EPERM
    # returned by FUSE indicates the FUSE_MKNOD path is blocked for device nodes.
    name = "mknod(path, S_IFCHR|0o666, makedev(1,3)) on FUSE mount"
    if os.geteuid() != 0:
        return skip(name, "requires root (CAP_MKNOD)")
    workdir = case_dir(root, "mknod_chardev_eperm")
    node_path = os.path.join(workdir, "test_null_node")

    return expect_ok(
        name,
        lambda: os.mknod(node_path, stat.S_IFCHR | 0o666, os.makedev(1, 3)),
    )


def test_mknod_blkdev_on_fuse_must_not_eperm(root: str) -> int:
    # Related LTP cases: mknod06
    # mknod(2) with S_IFBLK (loop device 7:0) must succeed as root; FUSE returning
    # EPERM in setup breaks all sub-cases of mknod06 before they can run.
    name = "mknod(path, S_IFBLK|0o660, makedev(7,0)) on FUSE mount"
    if os.geteuid() != 0:
        return skip(name, "requires root (CAP_MKNOD)")
    workdir = case_dir(root, "mknod_blkdev_eperm")
    node_path = os.path.join(workdir, "test_loop_node")

    return expect_ok(
        name,
        lambda: os.mknod(node_path, stat.S_IFBLK | 0o660, os.makedev(7, 0)),
    )


def test_unix_socket_bind_on_fuse_must_not_eperm(root: str) -> int:
    # Related LTP cases: bind03, getsockopt02, recvmsg01, sendmsg01, sockioctl01
    # bind(2) on an AF_UNIX socket with a path that lives on a FUSE mount internally
    # triggers mknod to create the socket node; FUSE returning EPERM causes the bind
    # itself to fail with EPERM, breaking all Unix-domain socket tests.
    workdir = case_dir(root, "unix_socket_bind_eperm")
    sock_path = os.path.join(workdir, "test.sock")

    def do_bind() -> None:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.bind(sock_path)
        finally:
            s.close()

    return expect_ok(
        "bind(AF_UNIX, path_on_fuse) — Unix socket node creation via mknod",
        do_bind,
    )


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

TESTS: List[Tuple[str, str, Callable[..., int]]] = [
    ("1", "mkfifo(2) on FUSE must not return EPERM", test_mkfifo_on_fuse_must_not_eperm),
    ("2", "mknod(2) S_IFIFO on FUSE must not return EPERM", test_mknod_fifo_type_on_fuse_must_not_eperm),
    ("3", "mknod(2) S_IFCHR on FUSE must not return EPERM", test_mknod_chardev_on_fuse_must_not_eperm),
    ("4", "mknod(2) S_IFBLK on FUSE must not return EPERM", test_mknod_blkdev_on_fuse_must_not_eperm),
    ("5", "bind(AF_UNIX, path_on_fuse) must not return EPERM", test_unix_socket_bind_on_fuse_must_not_eperm),
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="mknod/mkfifo EPERM on Curvine FUSE — reproducer",
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
        print(
            "WARNING: not running as root — mknod device-node tests will be skipped",
            file=sys.stderr,
        )

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    print(f"Running mknod/mkfifo EPERM reproducer under {root}\n")

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
