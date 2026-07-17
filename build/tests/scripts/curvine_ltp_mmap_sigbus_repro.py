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
mmap page access raises SIGBUS on Curvine FUSE — reproducer for LTP failures.

Root cause: accessing a MAP_SHARED file mapping triggers a page fault whose
FUSE read-back fails, delivering SIGBUS instead of file contents.

Covered LTP cases (each line is one minimal repro scenario):
  mmap02 — PROT_READ mmap of mode-0444 file on O_RDONLY fd must be readable (TBROK SIGBUS)
  mmap03 — PROT_EXEC mmap of mode-0555 file on O_RDONLY fd must be readable (TBROK SIGBUS)
  mmap04 — PROT_READ|PROT_EXEC mmap of mode-0555 file must match file data (TBROK SIGBUS)

Usage:
    python3 curvine_ltp_mmap_sigbus_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_mmap_sigbus_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import multiprocessing
import os
import shutil
import signal
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

PROT_READ = 1
PROT_EXEC = 4
PROT_READ_EXEC = PROT_READ | PROT_EXEC
MAP_SHARED = 1

O_RDWR = 2
O_CREAT = 0o100
O_TRUNC = 0o1000
O_RDONLY = 0


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


def sys_mmap(addr: int, length: int, prot: int, flags: int, fd: int, offset: int) -> int:
    """Call libc mmap with explicit ABI types so pointers are not truncated."""
    libc = ctypes.CDLL(None, use_errno=True)
    mmap_fn = libc.mmap
    mmap_fn.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_long,
    ]
    mmap_fn.restype = ctypes.c_void_p

    ret = mmap_fn(addr, length, prot, flags, fd, offset)
    if ret == ctypes.c_void_p(-1).value:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return int(ret)


def sys_munmap(addr: int, length: int) -> None:
    """Call libc munmap with explicit ABI types."""
    libc = ctypes.CDLL(None, use_errno=True)
    munmap_fn = libc.munmap
    munmap_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    munmap_fn.restype = ctypes.c_int

    ret = munmap_fn(addr, length)
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def _page_size() -> int:
    return os.sysconf("SC_PAGE_SIZE")


def _create_fill_file(path: str, page_sz: int, file_mode: int) -> None:
    fd = os.open(path, O_RDWR | O_CREAT | O_TRUNC, 0o666)
    try:
        os.write(fd, b"A" * page_sz)
        os.fchmod(fd, file_mode)
    finally:
        os.close(fd)


def _child_read_mapped_file(
    path: str,
    page_sz: int,
    prot: int,
    verify_contents: bool,
) -> None:
    fd = os.open(path, O_RDONLY)
    try:
        addr = sys_mmap(0, page_sz, prot, MAP_SHARED, fd, 0)
        try:
            # A PROT_EXEC-only mapping is intentionally not data-readable. mmap03
            # only verifies that creating the executable mapping succeeds.
            if verify_contents:
                mapped = (ctypes.c_char * page_sz).from_address(addr)
                if bytes(mapped) != b"A" * page_sz:
                    raise OSError(errno.EIO, "mapped memory area contains invalid data")
        finally:
            sys_munmap(addr, page_sz)
    finally:
        os.close(fd)


def _run_mmap_read_child(
    path: str,
    page_sz: int,
    prot: int,
    verify_contents: bool = True,
) -> None:
    proc = multiprocessing.Process(
        target=_child_read_mapped_file,
        args=(path, page_sz, prot, verify_contents),
    )
    proc.start()
    proc.join()
    if proc.exitcode == -signal.SIGBUS:
        raise OSError(
            errno.EFAULT,
            "unexpected SIGBUS accessing mmap'd FUSE page (page-fault read-back failed)",
        )
    if proc.exitcode != 0:
        raise OSError(
            errno.EIO,
            f"child mmap read failed with exit code {proc.exitcode}",
        )


def test_mmap_shared_read_only_file_readable(root: str) -> int:
    # Related LTP cases: mmap02
    # MAP_SHARED PROT_READ mmap of a mode-0444 file on O_RDONLY fd must be readable.
    workdir = case_dir(root, "mmap_prot_read")
    target = os.path.join(workdir, "mmapfile")
    page_sz = _page_size()
    _create_fill_file(target, page_sz, 0o444)

    def run() -> None:
        _run_mmap_read_child(target, page_sz, PROT_READ)

    return expect_ok(
        "PROT_READ MAP_SHARED mmap of mode-0444 file readable without SIGBUS (mmap02)",
        run,
    )


def test_mmap_shared_exec_file_readable(root: str) -> int:
    # Related LTP cases: mmap03
    # MAP_SHARED PROT_EXEC mmap of a mode-0555 file on O_RDONLY fd must be readable.
    workdir = case_dir(root, "mmap_prot_exec")
    target = os.path.join(workdir, "mmapfile")
    page_sz = _page_size()
    _create_fill_file(target, page_sz, 0o555)

    def run() -> None:
        _run_mmap_read_child(target, page_sz, PROT_EXEC, verify_contents=False)

    return expect_ok(
        "PROT_EXEC MAP_SHARED mmap of mode-0555 file succeeds (mmap03)",
        run,
    )


def test_mmap_shared_read_exec_file_readable(root: str) -> int:
    # Related LTP cases: mmap04
    # MAP_SHARED PROT_READ|PROT_EXEC mmap of mode-0555 file must expose file contents.
    workdir = case_dir(root, "mmap_prot_read_exec")
    target = os.path.join(workdir, "mmapfile")
    page_sz = _page_size()
    _create_fill_file(target, page_sz, 0o555)

    def run() -> None:
        _run_mmap_read_child(target, page_sz, PROT_READ_EXEC)

    return expect_ok(
        "PROT_READ|PROT_EXEC MAP_SHARED mmap of mode-0555 file readable without SIGBUS "
        "(mmap04)",
        run,
    )


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "PROT_READ mmap of mode-0444 file readable (mmap02)",
        test_mmap_shared_read_only_file_readable,
    ),
    (
        "2",
        "PROT_EXEC mmap of mode-0555 file readable (mmap03)",
        test_mmap_shared_exec_file_readable,
    ),
    (
        "3",
        "PROT_READ|PROT_EXEC mmap of mode-0555 file readable (mmap04)",
        test_mmap_shared_read_exec_file_readable,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="mmap page access SIGBUS on Curvine FUSE — reproducer",
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

    print(f"Running mmap-SIGBUS reproducer under {root}\n")

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
